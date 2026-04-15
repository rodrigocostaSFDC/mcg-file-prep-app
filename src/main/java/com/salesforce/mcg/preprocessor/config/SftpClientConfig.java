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

package com.salesforce.mcg.preprocessor.config;

import com.jcraft.jsch.JSch;
import com.jcraft.jsch.JSchException;
import com.jcraft.jsch.Session;
import com.salesforce.mcg.preprocessor.properties.SftpPreprocessorProperties;
import com.salesforce.mcg.preprocessor.service.LocalEmbeddedSftpServer;
import com.salesforce.mcg.preprocessor.util.SftpPropertyContext;
import lombok.extern.slf4j.Slf4j;
import org.apache.logging.log4j.util.Strings;
import org.springframework.beans.factory.BeanCreationException;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.condition.ConditionalOnClass;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.integration.sftp.session.DefaultSftpSessionFactory;
import org.springframework.integration.sftp.session.SftpRemoteFileTemplate;

import org.apache.sshd.client.SshClient;
import org.apache.sshd.client.keyverifier.AcceptAllServerKeyVerifier;
import org.apache.sshd.core.CoreModuleProperties;

import java.time.Duration;
import java.util.Properties;

import static com.salesforce.mcg.preprocessor.common.AppConstants.*;

/**
 * Configures the JSch {@link Session} (used by {@code SftpService} for file
 * listings and downloads) and the Spring Integration
 * {@link SftpRemoteFileTemplate} (used for streaming output file uploads).
 *
 * <p>The {@code local} profile variant retries the initial TCP connection up
 * to five times to allow a locally started SFTP server to become ready.
 */
@Configuration
@ConditionalOnClass(JSch.class)
@EnableConfigurationProperties(SftpPreprocessorProperties.class)
@Slf4j
public class SftpClientConfig {

    @Value("${sftp.mode}")
    private String sftpMode;

    @ConditionalOnProperty(name = SFTP_MODE, havingValue = MODE_LOCAL)
    @Bean JSch jSchLocal(
            SftpPropertyContext context,
            LocalEmbeddedSftpServer sftpServer) throws JSchException {
        var props = context.getPropertiesForActiveCompany();
        var jsch = new JSch();
        var session = jsch.getSession(props.username(), props.host(), props.port());
        session.setPassword(props.password());
        var config = new Properties();
        config.put(STRICT_HOST_KEY_CHECKING, STRICT_NO);
        session.setConfig(config);
        session.connect();
        return jsch;
    }

    @Bean
    @ConditionalOnProperty(name = SFTP_MODE, havingValue = MODE_REMOTE, matchIfMissing = true)
    public JSch jschRemote(SftpPropertyContext context) throws JSchException {
        var props = context.getPropertiesForActiveCompany();
        var jsch = new JSch();
        if (Strings.isNotBlank(props.knownHosts())) {
            jsch.setKnownHosts(props.knownHosts());
        }
        if (Strings.isNotBlank(props.privateKey())) {
            if (props.passphrase() != null && !props.passphrase().isBlank()) {
                jsch.addIdentity(props.privateKey(), props.passphrase());
            } else {
                jsch.addIdentity(props.privateKey());
            }
        }
        return jsch;
    }

    @Bean(destroyMethod = "disconnect")
    public Session sftpSession(JSch jsch, SftpPropertyContext context) {
        try {
            return buildAndConnectSession(jsch, context);
        } catch (JSchException e) {
            throw new BeanCreationException("❌ Failed to create SFTP session", e);
        }
    }

    @Bean(destroyMethod = "stop")
    public SshClient sshClient(SftpPropertyContext context) {
        var props = context.getPropertiesForActiveCompany();
        var client = SshClient.setUpDefaultClient();
        CoreModuleProperties.HEARTBEAT_INTERVAL.set(client,
                Duration.ofMillis(props.serverAliveInterval()));
        CoreModuleProperties.HEARTBEAT_REPLY_WAIT.set(client,
                Duration.ofMillis(props.setTimeout()));
        client.addPasswordIdentity(props.password());
        if (props.allowUnknownKeys()) {
            client.setServerKeyVerifier(AcceptAllServerKeyVerifier.INSTANCE);
        }
        client.start();
        return client;
    }

    @Bean
    public DefaultSftpSessionFactory sftpSessionFactory(SshClient sshClient, SftpPropertyContext context) {
        var props = context.getPropertiesForActiveCompany();
        var factory = new DefaultSftpSessionFactory(sshClient, true);
        factory.setHost(props.host());
        factory.setPort(props.port());
        factory.setUser(props.username());
        factory.setTimeout(props.setTimeout());
        return factory;
    }

    @Bean
    public SftpRemoteFileTemplate sftpRemoteFileTemplate(DefaultSftpSessionFactory factory) {
        return new SftpRemoteFileTemplate(factory);
    }

    // -------------------------------------------------------------------------

    private Session buildAndConnectSession(JSch jsch, SftpPropertyContext context) throws JSchException {
        var preferredAuthentications = MODE_LOCAL.equals(sftpMode) ? LOCAL_PREFERRED_AUTH: REMOTE_PREFERRED_AUTH;
        var props = context.getPropertiesForActiveCompany();
        var session = jsch.getSession(props.username(), props.host(), props.port());
        session.setPassword(props.password());
        if ((props.privateKey() == null || props.privateKey().isBlank())
                && props.passphrase() != null) {
            session.setPassword(props.password());
        }
        var cfg = new Properties();
        var strictHostKey = !props.allowUnknownKeys();
        cfg.put(STRICT_HOST_KEY_CHECKING, strictHostKey ? STRICT_YES : STRICT_NO);
        cfg.put(PREFERRED_AUTHENTICATIONS, preferredAuthentications);
        session.setConfig(cfg);
        session.setServerAliveInterval(props.serverAliveInterval());
        session.setServerAliveCountMax(props.setServerAliveCountMax());
        session.setTimeout(props.setTimeout());
        session.connect(15_000);
        return session;
    }

}
