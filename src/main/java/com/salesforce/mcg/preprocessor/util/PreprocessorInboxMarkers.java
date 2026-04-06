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

import org.apache.logging.log4j.util.Strings;

import java.util.Locale;

import static com.salesforce.mcg.preprocessor.common.AppConstants.*;

/**
 * Inbox naming rules for the file preprocessor (aligned with {@code mcg-gateway-app}
 * {@code com.salesforce.mcg.gateway.util.PreprocessorInboxMarkers}).
 * <ul>
 *   <li>Only {@code .zip} and {@code .txt} extensions are eligible for processing/listing.
 *   <li>Failed runs are renamed with .hasError, which drops them out of the
 *       {@code .zip}/{@code .txt} rule (e.g. {@code job.zip.hasErrors}).
 * </ul>
 */
public final class PreprocessorInboxMarkers {

    /** Appended to the full original filename, e.g. {@code job_S_1.zip.hasErrors}. */

    private PreprocessorInboxMarkers() {}

    public static boolean isHasErrorsMarked(String filename) {
        return filename != null && filename.toLowerCase(Locale.ROOT)
                .endsWith(FILENAME_ERRORS_SUFFIX.toLowerCase(Locale.ROOT));
    }

    /**
     * Normalizes a runtime {@code --file} argument to its leaf filename.
     * Accepts either a bare name ({@code test.txt}) or a path-like input
     * ({@code inbox/test.txt}, {@code /inbox/test.txt}, {@code inbox\test.txt}).
     *
     * @param requestedFile raw runtime argument
     * @return normalized leaf filename, or {@code null} when input is null/blank
     */
    public static String normalizeRequestedInputFile(final String requestedFile) {
        if (Strings.isBlank(requestedFile)) {
            return null;
        }
        var normalized = requestedFile.replace(CHAR_BACKSLASH, CHAR_FORWARD_SLASH);
        int lastSlash = normalized.lastIndexOf(CHAR_FORWARD_SLASH);
        if (lastSlash >= 0) {
            if (lastSlash == normalized.length() - 1) {
                return null;
            }
            normalized = normalized.substring(lastSlash + 1);
        }
        return normalized;
    }

    /**
     * @return {@code original + .hasErrors}, or unchanged if already marked
     */
    public static String withHasErrorsMarker(String originalFileName) {
        if (originalFileName == null || originalFileName.isBlank()) {
            throw new IllegalArgumentException("❌ FileName must not be blank");
        }
        if (isHasErrorsMarked(originalFileName)) {
            return originalFileName;
        }
        return originalFileName + FILENAME_ERRORS_SUFFIX;
    }
}

