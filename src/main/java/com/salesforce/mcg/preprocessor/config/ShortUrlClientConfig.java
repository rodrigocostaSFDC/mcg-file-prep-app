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

import lombok.extern.slf4j.Slf4j;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.http.client.SimpleClientHttpRequestFactory;
import org.springframework.web.client.RestTemplate;

import java.util.Objects;

/**
 * Configures the {@link RestTemplate} used to call the
 * {@code mcg-short-url-app} batch endpoint.
 *
 * <p>Mirrors {@code ShortUrlClientConfig} from {@code mcg-processor-app}.
 * SSL is handled at the JVM level via system properties
 * ({@code javax.net.ssl.trustStore} etc.) when running on Heroku.
 */
@Configuration
@EnableConfigurationProperties(ShortUrlClientConfig.ShortUrlProperties.class)
@Slf4j
public class ShortUrlClientConfig {

    public static final String SHORT_URL_REST_TEMPLATE = "shortUrlRestTemplate";
    public static final String PROPERTY_PREFIX_SHORTURL = "shorturl";

    @Bean(SHORT_URL_REST_TEMPLATE)
    public RestTemplate shortUrlRestTemplate(ShortUrlProperties props) {
        SimpleClientHttpRequestFactory factory = new SimpleClientHttpRequestFactory();
        factory.setConnectTimeout(Objects.requireNonNullElse(props.connectTimeoutMs(), 3000));
        factory.setReadTimeout(Objects.requireNonNullElse(props.readTimeoutMs(), 5000));
        return new RestTemplate(factory);
    }

    /**
     *
     * @param baseUrl base url de SFTP server
     * @param batchEndpoint
     * @param connectTimeoutMs
     * @param readTimeoutMs
     * @param enableDefaultHttpProtocolWhenMissing
     * @param useSecureHttpProtocolByDefault
     */
    @ConfigurationProperties(prefix = PROPERTY_PREFIX_SHORTURL)
    public record ShortUrlProperties(
            String baseUrl,
            String batchEndpoint,
            Integer connectTimeoutMs,
            Integer readTimeoutMs,
            boolean enableDefaultHttpProtocolWhenMissing,
            boolean useSecureHttpProtocolByDefault){}
}
