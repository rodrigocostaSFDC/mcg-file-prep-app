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

import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import java.util.concurrent.atomic.AtomicInteger;

/**
 * Assigns short codes to subscribers using the same strategy as
 * {@code GatewayService} in {@code mcg-gateway-app}.
 *
 * <ul>
 *   <li>Telcel subscribers always receive {@link #telcelShortCode}.
 *   <li>Non-Telcel subscribers are round-robin distributed between
 *       {@link #nonTelcelShortCode1} and {@link #nonTelcelShortCode2}.
 * </ul>
 *
 * All short code values are configurable via environment variables so they
 * can be updated without a code change.
 */
@Component
public class ShortCodeBalancer {

    @Value("${shortcode.telcel:89992}")
    private String telcelShortCode;

    @Value("${shortcode.non-telcel.1:35000}")
    private String nonTelcelShortCode1;

    @Value("${shortcode.non-telcel.2:90120}")
    private String nonTelcelShortCode2;

    /** Counter drives the round-robin selection for non-Telcel subscribers. */
    private final AtomicInteger counter = new AtomicInteger(0);

    /**
     * Returns the appropriate short code for the subscriber.
     *
     * @param isTelcel {@code true} if the subscriber belongs to Telcel
     * @return the short code string to embed in the output row
     */
    public String assignShortCode(boolean isTelcel) {
        if (isTelcel) {
            return telcelShortCode;
        }
        return (counter.incrementAndGet() % 2 == 0) ? nonTelcelShortCode1 : nonTelcelShortCode2;
    }
}
