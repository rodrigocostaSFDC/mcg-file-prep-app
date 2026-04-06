package com.salesforce.mcg.preprocessor.helper;

import com.jcraft.jsch.ChannelSftp;
import org.junit.jupiter.api.Test;

import java.io.ByteArrayInputStream;
import java.lang.reflect.Constructor;
import java.lang.reflect.Method;
import java.util.regex.Pattern;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

class SftpClientHelperTest {

    @Test
    void channelClosingInputStream_readsAndDisconnectsOnce() throws Exception {
        byte[] payload = "abc".getBytes();
        ByteArrayInputStream delegate = new ByteArrayInputStream(payload);
        ChannelSftp channel = mock(ChannelSftp.class);
        SftpClientHelper.ChannelClosingInputStream in = new SftpClientHelper.ChannelClosingInputStream(delegate, channel);

        assertThat(in.read()).isEqualTo('a');
        assertThat(in.available()).isEqualTo(2);
        assertThatCode(in::close).doesNotThrowAnyException();
        assertThatCode(in::close).doesNotThrowAnyException();
        verify(channel).disconnect();
    }

    @Test
    void privateToRegex_supportsWildcardsAndEscaping() throws Exception {
        Method toRegex = SftpClientHelper.class.getDeclaredMethod("toRegex", String.class);
        toRegex.setAccessible(true);
        Pattern p = (Pattern) toRegex.invoke(null, "CA*_S_?.txt");

        assertThat(p.matcher("CA123_S_1.txt").matches()).isTrue();
        assertThat(p.matcher("CA123_S_12.txt").matches()).isFalse();
    }

    @Test
    void helperPrivateConstructor_isInvocable() throws Exception {
        Constructor<SftpClientHelper> ctor = SftpClientHelper.class.getDeclaredConstructor();
        ctor.setAccessible(true);
        assertThatCode(ctor::newInstance).doesNotThrowAnyException();
    }
}
