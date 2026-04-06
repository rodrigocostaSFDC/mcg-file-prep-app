/*****************************************************************************
 * DISCLAIMER:
 * This code is provided "AS IS", without any express or implied warranties,
 * including, but not limited to, the implied warranties of merchantability,
 * fitness for a particular purpose, or non-infringement. Use of this code is
 * at your own risk. In no event shall the authors or copyright holders be
 * liable for any direct, indirect, incidental, special, exemplary, or
 * consequential damages (including, but not limited to, procurement of
 * substitute goods or services; loss of use, data, or profits; or business
 * interruption), however caused and on any theory of liability, whether in
 * contract, strict liability, or tort (including negligence or otherwise)
 * arising in any way out of the use of this code, even if advised of the
 * possibility of such damage.
 ****************************************************************************/

package com.salesforce.mcg.preprocessor.service;

import com.jcraft.jsch.ChannelSftp;
import com.jcraft.jsch.JSchException;
import com.jcraft.jsch.Session;
import com.jcraft.jsch.SftpException;
import com.salesforce.mcg.preprocessor.properties.SftpServerProperties;
import jakarta.annotation.Resource;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.integration.sftp.session.SftpRemoteFileTemplate;
import org.springframework.stereotype.Service;

import com.salesforce.mcg.preprocessor.util.PreprocessorInboxMarkers;
import com.salesforce.mcg.preprocessor.util.ProcessedInputNaming;
import com.salesforce.mcg.preprocessor.util.SftpPropertyContext;

import java.io.InputStream;

import static com.salesforce.mcg.preprocessor.common.AppConstants.*;
import static com.salesforce.mcg.preprocessor.helper.SftpClientHelper.*;

/**
 * Low-level SFTP operations for the file-preprocessor dyno.
 *
 * <p>All transfer methods are <b>streaming</b> — neither the download nor the upload
 * accumulates the full file contents in heap memory.  This is critical for files in the
 * 5–15 M row range:
 *
 * <ul>
 *   <li>{@link #openDownloadStream(String)} — returns a live SFTP channel stream; the
 *       caller reads from it and closes it when done, which also closes the channel.
 *       {@link InputStream} directly to SFTP via
 *       {@link SftpRemoteFileTemplate} — no intermediate buffer.
 * </ul>
 */
@Service
@RequiredArgsConstructor
@Slf4j
public class SftpClientService {

    @Resource
    private Session sftpSession;

    private final SftpRemoteFileTemplate remoteFileTemplate;
    private final SftpPropertyContext sftpPropertyContext;

    /**
     * When true, after the upload to {@code tempName} completes, copy remote temp → final (streaming) and {@code rm} temp.
     * Marketing Cloud file-drop often treats this as a new upload; server-side rename alone may not fire automation.
     */
    @Value("${preprocessor.output.copy-instead-of-rename:false}")
    private boolean copyInsteadOfRename;

    /**
     * Opens a streaming download from the given remote path.
     *
     * <p>The SFTP channel is kept open for the duration of the transfer; it is closed
     * automatically when the returned stream is closed.  This means the caller's
     * {@code InputStream.close()} must be called (typically via try-with-resources) to
     * release the channel.
     *
     * <p><b>Important:</b> do not hold the stream open longer than necessary.  The JSch
     * session has a fixed server-alive timeout and will drop idle connections.
     *
     * @param remotePath full remote path to the file
     * @return live stream — caller owns the close
     */
    public InputStream openDownloadStream(String remotePath) throws JSchException, SftpException{

        try {
            if (!sftpSession.isConnected()) {
                sftpSession.connect();
            }
            var channel = getSftpChannel();
            log.info("📡 Opening download stream: {}", remotePath);
            var raw = channel.get(remotePath);
            return new ChannelClosingInputStream(raw, channel);
        } catch (JSchException | SftpException e){
            log.error("❌ Error opening download stream: %s".formatted(e.getMessage()));
            throw e;
        }
    }

    /**
     * Uploads to a temporary filename, then renames to the final name when done.
     * Use this to avoid partial files being visible under the final name.
     *
     * @param tempName   temporary filename (e.g. {@code temp1734567890123.txt} — plain text, not compressed;
     *                   intentionally not the final output name for MC Automation Studio)
     * @param finalName  final filename after successful upload
     * @param content    source stream
     */
    public void uploadStreamTempThenRename(String tempName, String finalName, InputStream content)
            throws SftpException, JSchException, java.io.IOException {
        var props = getProps();
        var tempPath = "%s/%s".formatted(props.outputDir(), tempName);
        var finalPath = "%s/%s".formatted(props.outputDir(), finalName);
        log.info("📡 Streaming upload to temp: {}", tempPath);
        try {
            remoteFileTemplate.execute(session -> {
                session.write(content, tempPath);
                return null;
            });
            if (copyInsteadOfRename) {
                log.info("⚙️ Copying remote temp to final (new-file semantics for MC), then removing temp");
                try (InputStream in = openDownloadStream(tempPath)) {
                    remoteFileTemplate.execute(session -> {
                        session.write(in, finalPath);
                        return null;
                    });
                }
                var channel = getSftpChannel();
                try {
                    channel.rm(tempPath);
                    log.info("✅ Copied {} -> {} and removed temp", tempPath, finalPath);
                } finally {
                    channel.disconnect();
                }
            } else {
                var channel = getSftpChannel();
                try {
                    channel.rename(tempPath, finalPath);
                    log.info("✅ Renamed {} -> {}", tempPath, finalPath);
                } finally {
                    channel.disconnect();
                }
            }
        } catch (Exception e){
            log.error("❌ Error while processing file: {}", e.getMessage(), e);
            throw e;
        }

    }

    /**
     * Moves the processed input file from {@link SftpServerProperties#inputDir()} to
     * {@code inputDir})} ()} (DONE), renaming the leaf to match
     * the READY output name per {@link ProcessedInputNaming} so inbox is empty and DONE/READY basenames align.
     *
     * @param inputFileName     original inbox filename (no path prefix)
     * @param readyOutputFileName final enriched filename under {@link SftpServerProperties#outputDir()}
     */
    public void moveInputToProcessed(String inputFileName, String readyOutputFileName) throws SftpException, JSchException {
        var props = getProps();
        var destLeaf = ProcessedInputNaming.processedLeafMatchingReady(
                inputFileName, readyOutputFileName);
        var srcPath = "%s/%s".formatted(props.inputDir(), inputFileName);
        var destPath = "%s/%s".formatted(props.outputDir(), destLeaf);
        var channel = getSftpChannel();
        try {
            try {
                channel.stat(props.outputDir());
            } catch (SftpException e) {
                if (e.id == ChannelSftp.SSH_FX_NO_SUCH_FILE) {
                    channel.mkdir(props.outputDir());
                    log.info("ℹ️ Created processed subfolder: {}", props.outputDir());
                } else {
                    throw e;
                }
            }
            channel.rename(srcPath, destPath);
            log.info("✅ Moved input to processed (DONE name aligned with READY '{}'): {} -> {}",
                    readyOutputFileName, srcPath, destPath);
        } finally {
            channel.disconnect();
        }
    }

    /**
     * Renames {@code inputDir}/{@code fileName} to the same directory with {@code .hasErrors} appended.
     */
    public void renameInputMarkHasErrors(String fileName) throws SftpException, JSchException {
        var props = getProps();
        if (PreprocessorInboxMarkers.isHasErrorsMarked(fileName)) {
            log.info("ℹ️ Input already has {} suffix — skipping rename: {}"
                    , FILENAME_ERRORS_SUFFIX, fileName);
            return;
        }
        var destName = PreprocessorInboxMarkers.withHasErrorsMarker(fileName);
        var srcPath = "%s/%s".formatted(props.inputDir(), fileName);
        var destPath = "%s/%s".formatted(props.outputDir(), destName);
        var channel = getSftpChannel();
        try {
            channel.rename(srcPath, destPath);
            log.info("ℹ️ Marked failed input: {} -> {}", srcPath, destPath);
        } finally {
            channel.disconnect();
        }
    }

    private ChannelSftp getSftpChannel() throws JSchException {
        var sftpChannel = (ChannelSftp) sftpSession.openChannel(CHANNEL_SFTP);
        if (!sftpChannel.isConnected()){
            sftpChannel.connect();
        }
        return sftpChannel;
    }

    private SftpServerProperties getProps() {
        return sftpPropertyContext
                .getPropertiesForActiveCompany();
    }

}
