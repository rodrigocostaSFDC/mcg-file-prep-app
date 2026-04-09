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

package com.salesforce.mcg.preprocessor.launcher;

import com.jcraft.jsch.ChannelSftp;
import com.jcraft.jsch.SftpException;
import com.salesforce.mcg.preprocessor.service.FileProcessorService;
import com.salesforce.mcg.preprocessor.service.GatewayCallbackService;
import com.salesforce.mcg.preprocessor.service.SftpClientService;
import com.salesforce.mcg.preprocessor.util.SftpPropertyContext;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.logging.log4j.util.Strings;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.ApplicationArguments;
import org.springframework.boot.ApplicationRunner;
import org.springframework.stereotype.Component;

import java.io.InputStream;
import java.io.PipedInputStream;
import java.io.PipedOutputStream;
import java.util.List;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.zip.ZipEntry;
import java.util.zip.ZipInputStream;

import static com.salesforce.mcg.preprocessor.common.AppConstants.*;

/**
 * Entry point for the file-preprocessor one-off dyno.
 *
 * <p>Implements {@link ApplicationRunner} so that Spring Boot invokes
 * {@link #run} immediately after context startup. When the work is done
 * (or a fatal error occurs) the application shuts down via
 * {@link System#exit} — terminating the Heroku dyno automatically.
 *
 * <h2>Execution flow</h2>
 * <ol>
 *   <li>Discover matching input files in the configured SFTP {@code inputDir}.
 *   <li>Process up to {@code preprocessor.concurrency} files in parallel.
 *   <li>For each file: download → process → upload enriched output to {@code outputDir}
 *       (optionally staged on local disk first — see {@code preprocessor.output.stage-to-disk-before-upload}).
 *   <li>Log a summary and exit with code {@code 0} (success) or {@code 1} (any failure).
 * </ol>
 *
 * <h2>Command-line arguments</h2>
 * <ul>
 *   <li>{@code --company=<telmex|telnor>} — required tenant selector used by SFTP/property context.</li>
 *   <li>{@code --file=<name>} — required input file inside {@code inputDir}.</li>
 * </ul>
 *
 * <h2>Parallel processing on Heroku</h2>
 * Set {@code PREPROCESSOR_CONCURRENCY=2} (or higher) to process multiple files at once
 * within a single dyno.  All files share the same DB connection pool and short-URL
 * service, so keep this number modest (2–4).  If you need to process very large file
 * sets simultaneously, scale horizontally by running multiple one-off dynos, each
 * pointed at a different file via {@code --file=<name>}.
 */
@Component
@RequiredArgsConstructor
@Slf4j
public class ApplicationLauncher implements ApplicationRunner {

    private final SftpClientService sftpClientService;
    private final FileProcessorService fileProcessorService;
    private final GatewayCallbackService gatewayCallback;
    private final SftpPropertyContext sftpPropertyContext;

    @Value("${app.auto-shutdown:true}")
    private boolean autoShutdown;

    @Override
    public void run(ApplicationArguments args) {

        log.info("🚀 Application starting...");

        // Best-effort unlock when the JVM shuts down (SIGTERM from e.g. heroku ps:restart, dyno cycle).
        // heroku ps:kill / ps:stop often maps to an immediate stop: the platform may SIGKILL the dyno,
        // in which case shutdown hooks never run — you will not see POST /api/preprocessor/next.
        // For a graceful test, use: heroku ps:restart --dyno-name run.NNNN
        var threadName = "preprocessor-shutdown-hook";
        Runtime.getRuntime().addShutdownHook(new Thread(gatewayCallback::notifyNext, threadName));

        var file = requireSingleOption(args, "file");
        var company = requireSingleOption(args, "company");

        AtomicInteger processed = new AtomicInteger(0);
        AtomicInteger failed = new AtomicInteger(0);

        try {
            process(file, company, processed, failed);
        } catch (Exception e) {
            log.error("❌ Fatal error in file-preprocessor launcher: {}", e.getMessage(), e);
            failed.incrementAndGet();
        } finally {
            log.info("🏁 Run complete — processed: {}, failed: {}", processed.get(), failed.get());
            if (autoShutdown) {
                scheduleShutdown(failed.get() > 0 ? 1 : 0);
            }
        }
    }

    private String requireSingleOption(ApplicationArguments args, String name) {
        List<String> values = args.getOptionValues(name);
        if (values == null || values.isEmpty() || values.get(0) == null || values.get(0).isBlank()) {
            throw new IllegalArgumentException("Missing required argument: --" + name);
        }
        return values.get(0).trim();
    }

    private void process(String fileName, String company,
                            AtomicInteger processed,
                            AtomicInteger failed) {
        var sftpProperties = sftpPropertyContext.getPropertiesForActiveCompany();
        var inputDir = sftpProperties.inputDir().replaceAll("/$", Strings.EMPTY);
        var remotePath = "%s/%s".formatted(inputDir, fileName);
        var runId = fileName.replaceAll(FILENAME_UNSUPPORTED_CHARS_REGEX,
                String.valueOf(CHAR_UNDERSCORE));
        var outputFileName = buildOutputFileName(fileName);
        log.info("ℹ️ Processing file: {}, company: {} (runId={})", remotePath, company, runId);

        /*
         * Full streaming pipeline — no full-file buffers in heap at any stage:
         *
         *   SFTP download (.zip or .txt)
         *       └─► If .zip: ZipInputStream → first .txt entry inside archive
         *           If .txt: raw stream
         *               └─► FileProcessorService  [background thread]
         *                       writes enriched pipe-delimited rows (plain .txt) to processorOut
         *                               └─► processorIn
         *                                       └─► SFTP upload (.txt)  [main thread]
         *
         * Two modes (see {@link #stageOutputToDiskBeforeUpload}):
         *   <ul>
         *     <li><b>Pipe:</b> processor streams to SFTP temp while rows are produced (low disk; final basename not written until temp is done).</li>
         *     <li><b>Disk staging (Heroku default):</b> processor finishes a local file, then streams it once to the
         *         <b>final</b> SFTP name (no remote {@code temp*.txt}). Ephemeral disk must fit the output.</li>
         *   </ul>
         * Remote: with disk staging, one stream to the <b>final</b> SFTP filename (no {@code temp*.txt} on server).
         * Pipe mode still uses {@code temp{ts}.txt} then rename/copy; {@code preprocessor.output.copy-instead-of-rename} applies there.
         */
        try (InputStream downloadStream = sftpClientService.openDownloadStream(remotePath)) {

            var processStream = openProcessStream(fileName, downloadStream);
            var tempOutputName = "temp" + System.currentTimeMillis() + ".txt";
            var errorRows = processAndUploadViaPipe(processStream, runId, tempOutputName, outputFileName);

            log.info("ℹ️ File {} complete — uploaded as {}, error rows: {}",
                    remotePath, outputFileName, errorRows);

        } catch (Exception e) {
            if (isRemoteFileMissing(e)) {
                log.warn("⚠️ Input file not found on SFTP (skipping without rename): {}", remotePath);
                return;
            }
            log.error("❌ Failed to process file {}: {}", remotePath, e.getMessage());
            failed.incrementAndGet();
            try {
                sftpClientService.renameInputMarkHasErrors(fileName);
            } catch (Exception renameEx) {
                log.error("❌ Could not rename failed input with .hasErrors (file may be picked again): {}",
                        renameEx.getMessage());
            }
            return;
        }

        // Output fully uploaded and download stream closed — safe to move input to processed dir
        try {
            sftpClientService.moveInputToProcessed(fileName, outputFileName);
        } catch (Exception ex) {
            log.warn("❌ Failed to move input to processed: {}", ex.getMessage());
        }
        processed.incrementAndGet();
    }

    private boolean isRemoteFileMissing(Throwable ex) {
        Throwable current = ex;
        while (current != null) {
            if (current instanceof SftpException sftpEx && sftpEx.id == ChannelSftp.SSH_FX_NO_SUCH_FILE) {
                return true;
            }
            String msg = current.getMessage();
            if (msg != null && msg.contains("❌ No such file or directory")) {
                return true;
            }
            current = current.getCause();
        }
        return false;
    }

    /**
     * Producer/consumer over a pipe: SFTP upload runs while the processor writes (bounded memory).
     */
    private long processAndUploadViaPipe(InputStream processStream, String runId,
            String tempOutputName, String outputFileName) throws Exception {

        var processorOut = new PipedOutputStream();
        var processorIn = new PipedInputStream(processorOut, 256 * 1024);
        var threadName = "processor-%s".formatted(runId);
        var pool = Executors.newSingleThreadExecutor(r -> new Thread(r, threadName));
        var processorFuture = pool.submit(() -> {
            try {
                return fileProcessorService.process(processStream, processorOut, runId);
            } finally {
                processorOut.close();
            }
        });

        pool.shutdown();

        try {
            sftpClientService.uploadStreamTempThenRename(tempOutputName, outputFileName, processorIn);
        } finally {
            processorIn.close();
        }

        return processorFuture.get();
    }

    /**
     * If {@code fileName} ends with {@code .zip}, wraps the raw SFTP download stream in a
     * {@link ZipInputStream} and advances to the first {@code .txt} entry inside the archive.
     * The {@link ZipInputStream} must NOT be closed by the caller — closing the underlying
     * {@code downloadStream} (owned by the try-with-resources in {@link #process}) is sufficient.
     *
     * <p>For plain {@code .txt} files the download stream is returned unchanged.
     *
     * @param fileName     the SFTP filename (used to detect .zip extension)
     * @param downloadStream the raw byte stream from SFTP
     * @return stream positioned at the start of the pipe-delimited content
     * @throws java.io.IOException if the zip contains no .txt entry
     */
    private InputStream openProcessStream(String fileName, InputStream downloadStream)
            throws java.io.IOException {

        if (!fileName.toLowerCase().endsWith(FILE_EXTENSION_ZIP)) {
            return downloadStream;
        }

        log.info("ℹ️ ZIP file detected — decompressing on-the-fly: {}", fileName);
        ZipInputStream zip = new ZipInputStream(downloadStream);
        ZipEntry entry;
        while ((entry = zip.getNextEntry()) != null) {
            if (!entry.isDirectory() && entry.getName().toLowerCase().endsWith(FILE_EXTENSION_TXT)) {
                log.info("ℹ️ Found TXT entry inside ZIP: {} ({} bytes compressed)",
                        entry.getName(), entry.getCompressedSize());
                return zip; // caller reads decompressed bytes directly from here
            }
            zip.closeEntry();
        }
        throw new java.io.IOException("❌ No .txt entry found inside ZIP file: " + fileName);
    }

    /**
     * Output is always a {@code .txt} on SFTP (pipe-delimited enriched file).
     * Strips {@code .zip} or {@code .txt} from the input basename before adding {@code .txt}.
     * Input filenames are assumed unique; no timestamp suffix is appended.
     */
    private String buildOutputFileName(String originalName) {
        String lower = originalName.toLowerCase();
        String base;
        if (lower.endsWith(FILE_EXTENSION_ZIP) || lower.endsWith(FILE_EXTENSION_TXT)) {
            base = originalName.substring(0, originalName.length() - 4);
        } else {
            int dot = originalName.lastIndexOf('.');
            base = dot > 0 ? originalName.substring(0, dot) : originalName;
        }
        return base + FILE_EXTENSION_TXT;
    }

    private void scheduleShutdown(int exitCode) {
        log.info("🔌 Initiating dyno shutdown with exit code {}", exitCode);
        new Thread(() -> {
            try {
                Thread.sleep(2000);
            } catch (InterruptedException ie) {
                Thread.currentThread().interrupt();
            }
            log.info("ℹ️ Exiting now");
            System.exit(exitCode);
        }, "shutdown-thread").start();
    }
}
