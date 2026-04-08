package com.salesforce.mcg.preprocessor.util;

import org.apache.logging.log4j.util.Strings;

import static com.salesforce.mcg.preprocessor.common.AppConstants.*;

/**
 * Aligns the filename used when moving inbox input to the processed (DONE) area with the
 * enriched file name on READY/output, so automation can pair them (same basename / stem).
 */
public final class ProcessedInputNaming {

    /**
     * @param inputFileName   original inbox name (e.g. {@code CA..._S_....zip} or {@code .txt})
     * @param readyOutputName final name written under output/READY (always {@code .txt} from preprocessor)
     * @return leaf name for the file under the processed (DONE) folder
     */
    public static String processedLeafMatchingReady(String inputFileName, String readyOutputName) {
        if (Strings.isBlank(readyOutputName)) {
            return inputFileName;
        }
        var in = inputFileName == null ? EMPTY_STRING : inputFileName;
        var lowerIn = in.toLowerCase();
        var lowerOut = readyOutputName.toLowerCase();

        // Plain .txt input: DONE name identical to READY output name (inbox is empty after move).
        if (lowerIn.endsWith(FILE_EXTENSION_TXT)) {
            return readyOutputName;
        }

        // ZIP (or other): keep original extension but use the same stem as READY (e.g. ready base_ts.txt ↔ done base_ts.zip).
        if (lowerOut.endsWith(FILE_EXTENSION_TXT)) {
            var stem = readyOutputName.substring(0, readyOutputName.length() - 4);
            if (lowerIn.endsWith(FILE_EXTENSION_ZIP)) {
                return stem + FILE_EXTENSION_ZIP;
            }
            int dot = in.lastIndexOf(CHAR_DOT);
            var ext = dot > 0 ? in.substring(dot) : EMPTY_STRING;
            return stem + ext;
        }

        return inputFileName;
    }
}
