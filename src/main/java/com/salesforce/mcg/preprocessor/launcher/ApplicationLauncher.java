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
import java.io.OutputStream;
import java.io.PipedInputStream;
import java.io.PipedOutputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.List;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Stream;
import java.util.zip.ZipEntry;
import java.util.zip.ZipInputStream;

import static com.salesforce.mcg.preprocessor.common.AppConstants.*;

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

    @Value("${preprocessor.output.stage-to-disk-before-upload:false}")
    private boolean stageOutputToDiskBeforeUpload;

    @Value("${preprocessor.output.staging-dir:}")
    private String outputStagingDir;

    @Override
    public void run(ApplicationArguments args) {

        log.info("🚀 Application starting...");

        var threadName = "preprocessor-shutdown-hook";
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            deleteOrphanStagingFilesBestEffort();
            gatewayCallback.notifyNext();
        }, threadName));

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

        try (InputStream downloadStream = sftpClientService.openDownloadStream(remotePath)) {

            var processStream = openProcessStream(fileName, downloadStream);
            var tempOutputName = "temp" + System.currentTimeMillis() + ".txt";

            long errorRows = stageOutputToDiskBeforeUpload
                    ? processAndUploadViaLocalStaging(processStream, runId, outputFileName)
                    : processAndUploadViaPipe(processStream, runId, tempOutputName, outputFileName);

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
            if (msg != null && msg.contains("No such file or directory")) {
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
     * Writes the full enriched file to local disk, then uploads from disk in one shot.
     * The upload streams from disk — the full file is not loaded into heap.
     */
    private long processAndUploadViaLocalStaging(InputStream processStream, String runId,
            String outputFileName) throws Exception {

        Path stagingDir = resolveOutputStagingDirectory();
        Path localFile = Files.createTempFile(stagingDir, "preprocessor-" + runId + "-", ".staging");
        log.info("ℹ️ Staging preprocessor output on disk: {}", localFile.toAbsolutePath());

        try {
            var pool = Executors.newSingleThreadExecutor(
                    r -> new Thread(r, "processor-" + runId));

            var processorFuture = pool.submit(() -> {
                try (OutputStream fos = Files.newOutputStream(localFile,
                        StandardOpenOption.TRUNCATE_EXISTING,
                        StandardOpenOption.WRITE,
                        StandardOpenOption.CREATE)) {
                    return fileProcessorService.process(processStream, fos, runId);
                }
            });

            pool.shutdown();
            long errorRows = processorFuture.get();

            sftpClientService.uploadOutputFile(outputFileName, localFile);

            return errorRows;
        } finally {
            try {
                Files.deleteIfExists(localFile);
            } catch (Exception e) {
                log.warn("⚠️ Could not delete local staging file {}: {}", localFile.toAbsolutePath(), e.getMessage());
            }
        }
    }

    private Path resolveOutputStagingDirectory() throws java.io.IOException {
        if (outputStagingDir != null && !outputStagingDir.isBlank()) {
            Path p = Path.of(outputStagingDir.trim());
            if (!p.isAbsolute()) {
                String cwd = System.getProperty("user.dir");
                p = Path.of(cwd).resolve(p);
            }
            Files.createDirectories(p);
            return p.toAbsolutePath().normalize();
        }
        return Path.of(System.getProperty("java.io.tmpdir")).toAbsolutePath().normalize();
    }

    private void deleteOrphanStagingFilesBestEffort() {
        if (!stageOutputToDiskBeforeUpload) {
            return;
        }
        try {
            Path dir = resolveOutputStagingDirectory();
            if (!Files.isDirectory(dir)) {
                return;
            }
            try (Stream<Path> stream = Files.list(dir)) {
                stream.filter(p -> {
                    String n = p.getFileName().toString();
                    return n.startsWith("preprocessor-") && n.endsWith(".staging");
                }).forEach(p -> {
                    try {
                        Files.deleteIfExists(p);
                        log.info("ℹ️ Shutdown hook: deleted orphan staging file {}", p);
                    } catch (Exception e) {
                        log.warn("⚠️ Shutdown hook: could not delete {}: {}", p, e.getMessage());
                    }
                });
            }
        } catch (Exception e) {
            log.warn("⚠️ Shutdown hook: staging cleanup skipped: {}", e.getMessage());
        }
    }

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
                return zip;
            }
            zip.closeEntry();
        }
        throw new java.io.IOException("❌ No .txt entry found inside ZIP file: " + fileName);
    }

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
