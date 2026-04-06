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

import lombok.extern.slf4j.Slf4j;
import org.apache.logging.log4j.util.Strings;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.http.HttpEntity;
import org.springframework.http.HttpHeaders;
import org.springframework.http.client.SimpleClientHttpRequestFactory;
import org.springframework.stereotype.Service;
import org.springframework.web.client.RestTemplate;

import java.util.Map;
import java.util.Objects;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Calls the gateway POST /api/preprocessor/next when processing is done,
 * so the gateway can list SFTP again and launch the next dyno.
 *
 * <p>Uses short timeouts (5s) so shutdown hooks don't block JVM exit on SIGTERM.
 * Idempotent: only the first call sends the request (avoids duplicate from success + shutdown hook).
 */
@Service
@Slf4j
public class GatewayCallbackService {

    private final String url;
    private final String secret;
    private final int timeout;
    private final AtomicBoolean notified = new AtomicBoolean(false);

    public GatewayCallbackService(
            @Value("${preprocessor.gateway.callback.url:}") String url,
            @Value("${preprocessor.gateway.callback.secret:}") String secret,
            @Value("${gateway.callback.timeout.seconds:5}") int timeout) {
        var baseUrl = url.replaceAll("/$", Strings.EMPTY);
        this.url = "%s/%s".formatted(baseUrl, "/api/preprocessor/next");
        this.secret = secret;
        this.timeout = timeout;
    }

    /**
     * Notifies the gateway that processing is done. Gateway will list SFTP and launch next dyno if any.
     * Call from both normal completion and shutdown hook (SIGTERM, OOM, etc.).
     * Idempotent: only the first invocation sends the HTTP request.
     */

    public void notifyNext() {

        log.info("📡 Shutdown hook: gateway callback service called to release preprocessor lock");

        if (Objects.isNull(url) || Strings.isBlank(url)){
            log.warn("⚠️ Shutdown hook: preprocessor.gateway.callback.url not set — cannot call gateway /next; run lock may stay held");
            return;
        }

        if (!notified.compareAndSet(false, true)) {
            log.warn("⚠️ Gateway /next callback already sent — skipping duplicate");
            return;
        }

        try {
            var response = getTemplate().postForEntity(url, buildBearerRequest(secret), Map.class);
            log.info("✅ status={}", response.getStatusCode());
        } catch (Exception e) {
            handleGatewayNotificationError(notified, e);
        }
    }

    private HttpEntity<Void> buildBearerRequest(String secret) {
        var headers = new HttpHeaders();
        headers.setBearerAuth(secret == null ? "" : secret);
        return new HttpEntity<>(headers);
    }

    private void handleGatewayNotificationError(AtomicBoolean notified, Exception e) {
        log.error("️❌ {}", e.getMessage(), e);
        notified.set(false);
    }

    public RestTemplate getTemplate(){
        var factory = new SimpleClientHttpRequestFactory();
        int ms = (int) TimeUnit.SECONDS.toMillis(timeout);
        factory.setConnectTimeout(ms);
        factory.setReadTimeout(ms);
        return new RestTemplate(factory);
    }
}
