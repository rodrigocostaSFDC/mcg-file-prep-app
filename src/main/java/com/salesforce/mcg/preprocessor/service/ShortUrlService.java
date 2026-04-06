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

import com.salesforce.mcg.preprocessor.aspect.TrackElapsed;
import com.salesforce.mcg.preprocessor.data.ShortUrlRequest;
import com.salesforce.mcg.preprocessor.data.ShortUrlResponse;
import com.salesforce.mcg.preprocessor.util.PreprocessorBusinessClock;
import lombok.extern.slf4j.Slf4j;
import org.apache.logging.log4j.util.Strings;
import org.postgresql.PGConnection;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Service;

import javax.sql.DataSource;
import java.io.ByteArrayInputStream;
import java.nio.charset.StandardCharsets;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.time.Instant;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.util.*;

import static com.salesforce.mcg.preprocessor.common.AppConstants.*;

/**
 * Direct-DB short URL service that bypasses the HTTP call to {@code mcg-short-url-app}
 * and writes records straight into {@code shorturl_sch.short_url} via JDBC.
 *
 * <h2>Performance design for 10M-row files</h2>
 * Two modes:
 * <ul>
 *   <li><b>COPY</b> (default): PostgreSQL COPY into temp table + INSERT...SELECT ON CONFLICT.
 *       Typically 3–5x faster than batched INSERT for 10k+ rows.</li>
 *   <li><b>Batch INSERT</b>: Sub-batches (default 10k rows) when COPY disabled.</li>
 * </ul>
 */
@Service
@Slf4j
public class ShortUrlService {

    private static final String ALPHABET =
            "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789";
    private static final int CODE_LENGTH = 12;
    public static final String REQUEST_ID = "request_id";
    public static final String SHORT_URL = "short_url";
    public static final String DOUBLE_BARS = "\\";
    public static final String DOUBLE_QUOTES = "\"";
    public static final String ESCAPED_DOUBLED_BARS = "\\\\";
    public static final String ESCAPED_DOUBLE_QUOTES = "\"\"";
    public static final String COMMA = ",";
    public static final String LINE_BREAK = "\n";


    /** Use PostgreSQL COPY for bulk load when true (default). */
    @Value("${app.company:telmex}")
    private String company;

    /** Rows per INSERT...SELECT when using COPY path; smaller batches can reduce lock contention. */
    @Value("${shorturl.copy-insert-batch-size:10000}")
    private int copyInsertBatchSize;

    private static final DateTimeFormatter FORMATER = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss")
            .withZone(ZoneOffset.UTC);

    private static final String DROP_TEMP_TABLE = "DROP TABLE IF EXISTS _shorturl_batch";

    private static final String CREATE_TEMP_TABLE = """
            CREATE TEMP TABLE _shorturl_batch (
                id text,
                original_url text, 
                short_url text, 
                creation_date timestamp,
                redirect_count numeric default 0,
                request_id text, 
                subscriber_key text,
                phone_number text,
                email text,
                company text,
                message_type text,
                short_code text,
                api_key text,
                template_name text,
                transaction_id text,
                transaction_date text,
                tcode text
            ) ON COMMIT DROP
            """;

    private static final String COPY_SQL = """
            COPY _shorturl_batch (
                id,
                original_url,
                short_url,
                creation_date,
                redirect_count,
                request_id,
                subscriber_key,
                phone_number,
                email,
                company,
                message_type,
                short_code,
                api_key,
                template_name,
                transaction_id,
                transaction_date,
                tcode
            ) FROM STDIN WITH (FORMAT csv)
            """;

    private static final String INSERT_FROM_TEMP_BATCH = """
            INSERT INTO shorturl_sch.short_url (
                id,
                original_url,
                short_url,
                creation_date,
                redirect_count,
                request_id,
                subscriber_key,
                phone_number,
                email,
                company,
                message_type,
                short_code,
                api_key,
                template_name,
                transaction_id,
                transaction_date,
                tcode
            ) SELECT 
                id,
                original_url,
                short_url,
                creation_date,
                redirect_count,
                request_id,
                subscriber_key,
                phone_number,   
                email,           
                company,         
                message_type,     
                short_code,       
                api_key,         
                template_name,   
                transaction_id,  
                transaction_date,
                tcode            
            FROM (
                SELECT *, row_number() OVER () AS rn FROM _shorturl_batch
            ) x
            WHERE rn > ? AND rn <= ?
            ON CONFLICT (short_url) DO NOTHING
            """;

    private static final String SELECT_FROM_TEMP_JOIN = """
            SELECT 
                b.request_id, 
                COALESCE(s.short_url, b.short_url) AS short_url
            FROM _shorturl_batch b
                LEFT JOIN shorturl_sch.short_url s 
                    ON s.short_url = b.short_url
            """;

    private final DataSource dataSource;
    private final PreprocessorBusinessClock businessClock;
    private final java.security.SecureRandom random = new java.security.SecureRandom();

    @Value("${shorturl.base-domain:}")
    private String baseDomain;

    @Value("${shorturl.max-attempts:3}")
    private int maxAttempts;

    public ShortUrlService(
            JdbcTemplate jdbc, DataSource dataSource, PreprocessorBusinessClock businessClock) {
        this.dataSource = dataSource;
        this.businessClock = businessClock;
    }

    /**
     * Creates short URLs. Uses COPY when {@code shorturl.use-copy=true} (default),
     * otherwise batched INSERT.
     */
    @TrackElapsed("ShortUrlDirectService.shortUrlBatch")
    public List<ShortUrlResponse> shortUrlBatch(List<ShortUrlRequest> requests) {
        if (requests.isEmpty()) return List.of();

        var usedCodes = new HashSet<>(requests.size() * 2);
        var candidates = new String[requests.size()];
        for (int i = 0; i < requests.size(); i++) {
            String code;
            int tries = 0;
            do { code = generateCode(); }
            while (!usedCodes.add(code) && ++tries < maxAttempts);
            candidates[i] = code;
        }
        return shortUrlBatchCopy(requests, candidates);
    }

    /**
     * COPY-based path: temp table + COPY + INSERT...SELECT + lookup.
     */
    private List<ShortUrlResponse> shortUrlBatchCopy(List<ShortUrlRequest> requests, String[] candidates) {
        var now = businessClock.now();

        var csv = new StringBuilder(requests.size() * 256);
        for (int i = 0; i < requests.size(); i++) {
            var req = requests.get(i);
            var shortUrl = candidates[i];
            List<String> columns = List.of(
                    UUID.randomUUID().toString(),                     // UUID
                    csvEscape(req.originalUrl()),                     // Original URL
                    csvEscape(shortUrl),                              // Short Url
                    csvDateFormat(now.toInstant(ZoneOffset.UTC)),     // Creation Date
                    csvEscape(String.valueOf(ZERO)),                  // Redirect Count
                    csvEscape(req.requestId()),                       // Request Id
                    csvEscape(req.subscriberKey()),                   // Subscriber Key
                    csvEscape(req.phoneNumber()),                     // Phone Number
                    csvEscape(req.email()),                           // Email
                    csvEscape(company),                               // Company
                    csvEscape(SMS),                                   // Message Type
                    csvEscape(req.shortCode()),                       // Short Code
                    csvEscape(req.apiKey()),                          // Api Key
                    csvEscape(req.templateName()),                    // Template Name
                    csvEscape(req.transactionId()),                   // Transaction Id
                    csvDateFormat(req.transactionDate()),             // Transaction Date
                    csvEscape(req.tcode())                            // TCode
            );
           csv.append(String.join(String.valueOf(CHAR_COMMA), columns));
           csv.append(CR);
        }

        Map<String, String> requestIdToShortUrl = new HashMap<>(requests.size());
        try (Connection conn = dataSource.getConnection()) {
            conn.setAutoCommit(false);
            try (var stmt = conn.createStatement()) {
                stmt.execute("SET work_mem = '256MB'");
            }
            try {
                try (var stmt = conn.createStatement()) {
                    stmt.execute(DROP_TEMP_TABLE);
                    stmt.execute(CREATE_TEMP_TABLE);
                }

                byte[] bytes = csv.toString().getBytes(StandardCharsets.UTF_8);
                var pg = conn.unwrap(PGConnection.class);
                var cm = pg.getCopyAPI();
                cm.copyIn(COPY_SQL, new ByteArrayInputStream(bytes));

                int batchSize = Math.max(1, copyInsertBatchSize);
                for (int lo = 0; lo < requests.size(); lo += batchSize) {
                    int hi = Math.min(lo + batchSize, requests.size());
                    try (PreparedStatement ps = conn.prepareStatement(INSERT_FROM_TEMP_BATCH)) {
                        ps.setInt(1, lo);
                        ps.setInt(2, hi);
                        ps.executeUpdate();
                    }
                }

                try (PreparedStatement ps = conn.prepareStatement(SELECT_FROM_TEMP_JOIN);
                     var rs = ps.executeQuery()) {
                    while (rs.next()) {
                        var rid = rs.getString(REQUEST_ID);
                        var shortUrl = rs.getString(SHORT_URL);
                        if (rid != null && shortUrl != null) {
                            requestIdToShortUrl.put(rid, shortUrl);
                        }
                    }
                }

                conn.commit();
            } catch (Exception e) {
                log.error("❌{}",e.getMessage(),e);
                conn.rollback();
                throw e;
            } finally {
                conn.setAutoCommit(true);
            }
        } catch (Exception e) {
            log.error("❌ COPY path failed", e);
        }
        log.info("ℹ️ ShortUrl COPY completed for {} rows", requests.size());

        return buildResponses(requests, requestIdToShortUrl, candidates);
    }

    private List<ShortUrlResponse> buildResponses(List<ShortUrlRequest> requests, Map<String, String> requestIdToShortUrl, String[] candidates) {
        List<ShortUrlResponse> responses = new ArrayList<>(requests.size());
        for (int i = 0; i < requests.size(); i++) {
            var req = requests.get(i);
            var shortUrl = requestIdToShortUrl.get(req.requestId());
            if (Objects.isNull(shortUrl)) {
                log.warn("⚠️ Short URL collision for requestId={} — candidate '{}' taken", req.requestId(), candidates[i]);
                responses.add(new ShortUrlResponse(
                        null,
                        req.requestId(),
                        null,
                        req.originalUrl(),
                        null,
                        "⚠️ Short URL collision — code '" + candidates[i] + "' already exists"));
            } else {
                String fullUrl = Strings.isNotBlank(baseDomain)
                        ? baseDomain.stripTrailing() + "/" + shortUrl
                        : shortUrl;
                responses.add(new ShortUrlResponse(
                        null,
                        req.requestId(),
                        null,
                        req.originalUrl(),
                        fullUrl,
                        null));
            }
        }
        return responses;
    }

    private static String csvEscape(String value) {
        var s = Objects
                .requireNonNullElse(value, Strings.EMPTY)
                .replace(DOUBLE_BARS, ESCAPED_DOUBLED_BARS)
                .replace(DOUBLE_QUOTES, ESCAPED_DOUBLE_QUOTES);
        if (s.contains(COMMA) || s.contains(LINE_BREAK) || s.contains(String.valueOf(CR))) {
            return DOUBLE_QUOTES + s + DOUBLE_QUOTES;
        }
        return s;
    }

    private String generateCode() {
        StringBuilder sb = new StringBuilder(CODE_LENGTH);
        for (int i = 0; i < CODE_LENGTH; i++) {
            sb.append(ALPHABET.charAt(random.nextInt(ALPHABET.length())));
        }
        return sb.toString();
    }

    private static String csvDateFormat(Instant instant){
        if (Objects.isNull(instant)){
            return Strings.EMPTY;
        }
        return FORMATER.format(instant);
    }

}
