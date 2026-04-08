package com.salesforce.mcg.preprocessor.service;

import com.jcraft.jsch.Channel;
import com.jcraft.jsch.ChannelSftp;
import com.jcraft.jsch.Session;
import com.jcraft.jsch.SftpException;
import com.salesforce.mcg.preprocessor.properties.SftpServerProperties;
import com.salesforce.mcg.preprocessor.util.SftpPropertyContext;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.integration.sftp.session.SftpRemoteFileTemplate;
import org.springframework.test.util.ReflectionTestUtils;

import java.io.ByteArrayInputStream;
import java.io.InputStream;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class SftpClientServiceTest {

    @Mock
    private SftpRemoteFileTemplate remoteFileTemplate;
    @Mock
    private SftpPropertyContext sftpPropertyContext;
    @Mock
    private Session session;
    @Mock
    private ChannelSftp channel;

    private SftpClientService service;

    @BeforeEach
    void setUp() throws Exception {
        service = new SftpClientService(remoteFileTemplate, sftpPropertyContext);
        ReflectionTestUtils.setField(service, "sftpSession", session);
        ReflectionTestUtils.setField(service, "copyInsteadOfRename", false);
    }

    private void stubSftpDefaults() throws Exception {
        when(session.openChannel("sftp")).thenReturn((Channel) channel);
    }

    private void stubProps() {
        when(sftpPropertyContext.getPropertiesForActiveCompany()).thenReturn(
                new SftpServerProperties("telmex", "localhost", 22, "u", "p", "", "", "", true, "/in", "/out", "", "*", 20000, 3, 30000));
    }

    @Test
    void openDownloadStream_connectsIfNeededAndClosesChannel() throws Exception {
        stubSftpDefaults();
        when(session.isConnected()).thenReturn(false);
        when(channel.get("/in/f.txt")).thenReturn(new ByteArrayInputStream("x".getBytes()));

        InputStream in = service.openDownloadStream("/in/f.txt");
        assertThat(in.read()).isEqualTo('x');
        in.close();

        verify(session).connect();
        verify(channel).disconnect();
    }

    @Test
    void openDownloadStream_propagatesSftpException() throws Exception {
        stubSftpDefaults();
        when(session.isConnected()).thenReturn(true);
        when(channel.get("/in/missing.txt"))
                .thenThrow(new SftpException(ChannelSftp.SSH_FX_NO_SUCH_FILE, "missing"));

        assertThatThrownBy(() -> service.openDownloadStream("/in/missing.txt"))
                .isInstanceOf(SftpException.class);
    }

    @Test
    void uploadTempThenRename_renamesWhenCopyDisabled() throws Exception {
        stubProps();
        stubSftpDefaults();
        when(remoteFileTemplate.execute(any())).thenReturn(null);

        service.uploadStreamTempThenRename("temp.txt", "final.txt", new ByteArrayInputStream("abc".getBytes()));

        verify(channel).rename("/out/temp.txt", "/out/final.txt");
    }

    @Test
    void moveInputToProcessed_createsProcessedSubdirUnderInputWhenMissing() throws Exception {
        when(sftpPropertyContext.getPropertiesForActiveCompany()).thenReturn(
                new SftpServerProperties("telmex", "localhost", 22, "u", "p", "", "", "", true, "/in", "/out", "done", "*", 20000, 3, 30000));
        stubSftpDefaults();
        var attrs = mock(com.jcraft.jsch.SftpATTRS.class);
        doThrow(new SftpException(ChannelSftp.SSH_FX_NO_SUCH_FILE, "missing")).when(channel).stat("/in/done");
        when(channel.stat("/in/a.txt")).thenReturn(attrs);

        service.moveInputToProcessed("a.txt", "b.txt");

        verify(channel).mkdir("/in/done");
        verify(channel).rename("/in/a.txt", "/in/done/b.txt");
    }

    @Test
    void moveInputToProcessed_whenInboxAlreadyGoneButDoneExists_skipsRename() throws Exception {
        when(sftpPropertyContext.getPropertiesForActiveCompany()).thenReturn(
                new SftpServerProperties("telmex", "localhost", 22, "u", "p", "", "", "", true, "/in", "/out", "done", "*", 20000, 3, 30000));
        stubSftpDefaults();
        var attrs = mock(com.jcraft.jsch.SftpATTRS.class);
        when(channel.stat("/in/done")).thenReturn(attrs);
        doThrow(new SftpException(ChannelSftp.SSH_FX_NO_SUCH_FILE, "missing")).when(channel).stat("/in/a.txt");
        when(channel.stat("/in/done/b.txt")).thenReturn(attrs);

        service.moveInputToProcessed("a.txt", "b.txt");

        verify(channel, never()).rename(anyString(), anyString());
    }

    @Test
    void moveInputToProcessed_whenRenameNoSuchFileButDoneExists_treatsAsSuccess() throws Exception {
        when(sftpPropertyContext.getPropertiesForActiveCompany()).thenReturn(
                new SftpServerProperties("telmex", "localhost", 22, "u", "p", "", "", "", true, "/in", "/out", "done", "*", 20000, 3, 30000));
        stubSftpDefaults();
        var attrs = mock(com.jcraft.jsch.SftpATTRS.class);
        when(channel.stat("/in/done")).thenReturn(attrs);
        when(channel.stat("/in/a.txt")).thenReturn(attrs);
        doThrow(new SftpException(ChannelSftp.SSH_FX_NO_SUCH_FILE, "missing")).when(channel)
                .rename("/in/a.txt", "/in/done/b.txt");
        when(channel.stat("/in/done/b.txt")).thenReturn(attrs);

        service.moveInputToProcessed("a.txt", "b.txt");

        verify(channel).rename("/in/a.txt", "/in/done/b.txt");
    }

    @Test
    void moveInputToProcessed_skipsWhenProcessedSubfolderBlank() throws Exception {
        stubProps();

        service.moveInputToProcessed("a.txt", "b.txt");

        verify(session, never()).openChannel("sftp");
    }

    @Test
    void renameInputMarkHasErrors_skipsAlreadyMarked() throws Exception {
        service.renameInputMarkHasErrors("a.txt.hasErrors");
        verify(session, never()).openChannel("sftp");
    }
}
