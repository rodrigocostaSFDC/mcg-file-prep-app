/*****************************************************************************
 * DISCLAIMER:
 * This code is provided "AS IS", without any express or implied warranties,
 * including, but not limited to, the implied warranties of merchantability,
 * fitness for a particular purpose, or non-infringement. Use of this code is
 * at your own risk. In no event shall the authors or copyright holders be
 * liable for any direct, indirect, incidental, special, exemplary, or
 * consequential damages (including, but not limited to, procurement of
 * substitute goods or services, loss of use, data, or profits, or business
 * interruption), however caused and on any theory of liability, whether in
 * contract, strict liability, or tort (including negligence or otherwise)
 * arising in any way out of the use of this code, even if advised of the
 * possibility of such damage.
 ****************************************************************************/

package com.salesforce.mcg.preprocessor.data;

import lombok.Builder;
import lombok.Data;

import java.time.Instant;

/**
 * Mutable row state used while enriching each input line.
 */
@Data
@Builder
public class ProcessedLine {
    private int rowIndex;
    private String[] originalColumns;
    private String id;
    private String originalUrl;
    private String shortUrl;
    private String requestId;
    private String subscriberKey;
    private String phoneNumber;
    private String email;
    private String company;
    private String messageType;
    private String apiKey;
    private String shortCode;
    private String templateName;
    private String transactionId;
    private Instant transactionDate;
    private String tcode;
    private boolean telcel;
    private boolean hasError;
    private String errorMessage;
}
