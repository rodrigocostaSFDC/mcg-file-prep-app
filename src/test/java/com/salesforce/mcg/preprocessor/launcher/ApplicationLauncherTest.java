package com.salesforce.mcg.preprocessor.launcher;

import com.jcraft.jsch.ChannelSftp;
import com.jcraft.jsch.SftpException;
import com.salesforce.mcg.preprocessor.properties.SftpServerProperties;
import com.salesforce.mcg.preprocessor.service.FileProcessorService;
import com.salesforce.mcg.preprocessor.service.GatewayCallbackService;
import com.salesforce.mcg.preprocessor.service.SftpClientService;
import com.salesforce.mcg.preprocessor.util.PreprocessorBusinessClock;
import com.salesforce.mcg.preprocessor.util.SftpPropertyContext;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.boot.DefaultApplicationArguments;
import org.springframework.test.util.ReflectionTestUtils;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.InputStream;
import java.time.LocalDateTime;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.zip.ZipEntry;
import java.util.zip.ZipOutputStream;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class ApplicationLauncherTest {

    @Mock
    private SftpClientService sftpClientService;
    @Mock
    private FileProcessorService fileProcessorService;
    @Mock
    private GatewayCallbackService gatewayCallbackService;
    @Mock
    private PreprocessorBusinessClock businessClock;
    @Mock
    private SftpPropertyContext sftpPropertyContext;

    private ApplicationLauncher launcher;

    @BeforeEach
    void setUp() {
        launcher = new ApplicationLauncher(
                sftpClientService,
                fileProcessorService,
                gatewayCallbackService,
                businessClock,
                sftpPropertyContext);
        ReflectionTestUtils.setField(launcher, "autoShutdown", false);
        ReflectionTestUtils.setField(launcher, "outputTimestampSuffix", false);
    }

    private void stubActiveSftpProperties() {
        when(sftpPropertyContext.getPropertiesForActiveCompany()).thenReturn(new SftpServerProperties(
                "telmex", "localhost", 22, "u", "p",
                "", "", "", true, "/inbox", "/out", "", "*", 20000, 3, 30000));
    }

    @Test
    void processOne_whenRemoteFileMissing_skipsRenameAndDoesNotIncrementFailed() throws Exception {
        stubActiveSftpProperties();
        when(sftpClientService.openDownloadStream("/inbox/HEROKU2345_S_DEMO.txt"))
                .thenThrow(new SftpException(ChannelSftp.SSH_FX_NO_SUCH_FILE, "No such file or directory"));

        AtomicInteger processed = new AtomicInteger(0);
        AtomicInteger failed = new AtomicInteger(0);

        ReflectionTestUtils.invokeMethod(launcher, "process", "HEROKU2345_S_DEMO.txt", "telmex", processed, failed);

        assertThat(processed.get()).isZero();
        assertThat(failed.get()).isZero();
        verify(sftpClientService, never()).renameInputMarkHasErrors("HEROKU2345_S_DEMO.txt");
        verify(sftpClientService, never()).moveInputToProcessed("HEROKU2345_S_DEMO.txt", "HEROKU2345_S_DEMO.txt");
    }

    @Test
    void processOne_whenProcessingFails_marksHasErrorsAndIncrementsFailed() throws Exception {
        stubActiveSftpProperties();
        when(sftpClientService.openDownloadStream("/inbox/HEROKU2345_S_DEMO.txt"))
                .thenThrow(new RuntimeException("boom"));

        AtomicInteger processed = new AtomicInteger(0);
        AtomicInteger failed = new AtomicInteger(0);

        ReflectionTestUtils.invokeMethod(launcher, "process", "HEROKU2345_S_DEMO.txt", "telmex", processed, failed);

        assertThat(processed.get()).isZero();
        assertThat(failed.get()).isEqualTo(1);
        verify(sftpClientService).renameInputMarkHasErrors("HEROKU2345_S_DEMO.txt");
    }

    @Test
    void buildOutputFileName_appendsTimestampWhenEnabled() {
        ReflectionTestUtils.setField(launcher, "outputTimestampSuffix", true);
        when(businessClock.now()).thenReturn(LocalDateTime.of(2026, 4, 1, 23, 30));

        String output = ReflectionTestUtils.invokeMethod(launcher, "buildOutputFileName", "HEROKU2345_S_DEMO.txt");

        assertThat(output).isEqualTo("HEROKU2345_S_DEMO_20260401_2330.txt");
    }

    @Test
    void openProcessStream_returnsSameStreamForTxtInput() {
        InputStream in = new ByteArrayInputStream("a|b\n1|2\n".getBytes());
        InputStream out = ReflectionTestUtils.invokeMethod(launcher, "openProcessStream", "input.txt", in);
        assertThat(out).isSameAs(in);
    }

    @Test
    void openProcessStream_readsFirstTxtEntryFromZip() throws Exception {
        ByteArrayOutputStream zipped = new ByteArrayOutputStream();
        try (ZipOutputStream zos = new ZipOutputStream(zipped)) {
            zos.putNextEntry(new ZipEntry("inside.txt"));
            zos.write("HELLO".getBytes());
            zos.closeEntry();
        }

        InputStream out = ReflectionTestUtils.invokeMethod(
                launcher, "openProcessStream", "input.zip", new ByteArrayInputStream(zipped.toByteArray()));

        byte[] buf = out.readNBytes(5);
        assertThat(new String(buf)).isEqualTo("HELLO");
    }

    @Test
    void openProcessStream_throwsWhenZipHasNoTxtEntry() throws Exception {
        ByteArrayOutputStream zipped = new ByteArrayOutputStream();
        try (ZipOutputStream zos = new ZipOutputStream(zipped)) {
            zos.putNextEntry(new ZipEntry("inside.csv"));
            zos.write("x".getBytes());
            zos.closeEntry();
        }

        assertThatThrownBy(() -> ReflectionTestUtils.invokeMethod(
                launcher, "openProcessStream", "input.zip", new ByteArrayInputStream(zipped.toByteArray())))
                .hasRootCauseMessage("❌ No .txt entry found inside ZIP file: input.zip");
    }

    @Test
    void run_whenCompanyOptionMissing_doesNotStartProcessing() throws Exception {
        var args = new DefaultApplicationArguments("--file=HEROKU2345_S_DEMO.txt");

        assertThatThrownBy(() -> launcher.run(args))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("Missing required argument: --company");

        verify(sftpClientService, never()).openDownloadStream("/inbox/HEROKU2345_S_DEMO.txt");
    }
}
