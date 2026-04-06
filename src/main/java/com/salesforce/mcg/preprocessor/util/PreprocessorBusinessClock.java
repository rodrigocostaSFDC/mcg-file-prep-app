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

package com.salesforce.mcg.preprocessor.util;

import lombok.Getter;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import java.time.LocalDateTime;
import java.time.ZoneId;

/**
 * Wall-clock in the configured business timezone (default {@link java.time.ZoneId#of(String) America/Mexico_City}),
 * used for output filenames, short-URL transaction fields, and DB timestamps — not UTC.
 */
@Getter
@Component
public class PreprocessorBusinessClock {

    private final ZoneId zone;

    /**
     * @param zoneId an {@link ZoneId} ID such as {@code America/Mexico_City}; Spring injects from
     *               {@code preprocessor.time-zone}
     */
    public PreprocessorBusinessClock(
            @Value("${preprocessor.time-zone:America/Mexico_City}") String zoneId) {
        this.zone = ZoneId.of(zoneId.strip());
    }

    /** Current local date-time in the business zone. */
    public LocalDateTime now() {
        return LocalDateTime.now(zone);
    }
}
