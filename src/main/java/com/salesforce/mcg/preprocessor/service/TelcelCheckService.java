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
import org.springframework.beans.factory.annotation.Value;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Service;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.*;
import java.util.stream.Collectors;

/**
 * JDBC service that determines whether a phone number belongs to Telcel.
 *
 * <p>Lookup strategy:
 * <ol>
 *   <li>Query {@code datasync_sch.subscriber_portability} (ported numbers).
 *   <li>If not found, query {@code datasync_sch.subscriber_series} (series-based).
 *   <li>If still not found, default to non-Telcel.
 * </ol>
 *
 * <p>Telcel operator codes: {@code TELCEL_OPERATOR_CODES} (defaults to {@code 006,330,188}).
 */
@Service
@Slf4j
public class TelcelCheckService {

    public static final String COLUMN_OPERATOR = "operator";
    public static final String COLUMN_VIRTUAL_OPERATOR = "virtual_operator";
    public static final String COLUMN_PHONE = "phone";

    public static final String CREATE_PREPROC_PHONES_SQL = """
        CREATE TEMP TABLE IF NOT EXISTS _preproc_phones (
            phone BIGINT PRIMARY KEY
        )
        ON COMMIT DELETE ROWS
        """;

    public static final String DELETE_PREPROC_PHONES_SQL = """
        DELETE FROM _preproc_phones
        """;

    public static final String INSERT_PREPROC_PHONES_SQL = """
       INSERT INTO _preproc_phones
           (phone) VALUES (?)
        ON CONFLICT DO NOTHING
       """;

    public static final String SELECT_PREPROC_PHONES_SQL = """
        SELECT
            t.phone,
            p.operator
        FROM _preproc_phones t
            JOIN datasync_sch.subscriber_portability p
                ON p.phone_number = t.phone
        """;

    private static final String SELECT_PORTABILITY_SQL = """
        SELECT operator
        FROM datasync_sch.subscriber_portability
        WHERE phone_number = ?
    """;

    private static final String SELECT_SERIES_QUERY = """
        SELECT
            operator,
            virtual_operator
        FROM datasync_sch.subscriber_series
        WHERE series_start <= ?
          AND series_end >= ? LIMIT 1
    """;

    private static final String SELECT_PREPROC_PHONES_WITH_JOIN_TABLES_SQL = """
        SELECT
            t.phone,
            s.operator,
            s.virtual_operator
        FROM _preproc_phones t
        LEFT JOIN datasync_sch.subscriber_portability p
            ON p.phone_number = t.phone
        LEFT JOIN LATERAL (
            SELECT operator, virtual_operator
            FROM datasync_sch.subscriber_series
            WHERE int8range(series_start, series_end, '[]') @> t.phone
            LIMIT 1
        ) s ON true
        WHERE p.phone_number IS NULL
        """;

    public static final String ANALYZE_PREPROC_PHONES_SQL = "ANALYZE _preproc_phones";

    private final JdbcTemplate jdbc;

    @Value("#{'${telcel.operator.codes:006,330,188}'.split(',')}")
    private List<String> telcelOperatorCodes;

    @Value("${gateway.sms.force-non-telcel:false}")
    private boolean forceNonTelcel;

    public TelcelCheckService(JdbcTemplate jdbc) {
        this.jdbc = jdbc;
    }

    public boolean isTelcel(long phoneNumber) {
        if (forceNonTelcel) return false;
        return checkPortability(phoneNumber).orElseGet(() -> checkSeries(phoneNumber));
    }

    /**
     * Batch variant — temp table + JOINs for portability and series lookups (2 round-trips).
     */
    public Map<Long, Boolean> isTelcelBatch(List<Long> phoneNumbers) {

        if (phoneNumbers.isEmpty()) return Collections.emptyMap();

        if (forceNonTelcel) {
            return phoneNumbers.stream().collect(Collectors.toMap(
                    item -> item,
                    item -> false
            ));
        }

        Map<Long, Boolean> result = new HashMap<>(phoneNumbers.size() * 2);

        List<Long> phones12 = phoneNumbers.stream()
                .map(this::formatPhoneWithPrefix)
                .toList();

        Map<Long, Long> phone12to10 = new HashMap<>(phoneNumbers.size() * 2);
        for (int i = 0; i < phoneNumbers.size(); i++) {
            phone12to10.put(phones12.get(i), phoneNumbers.get(i));
        }

        jdbc.execute((java.sql.Connection conn) -> {
            boolean prevAutoCommit = conn.getAutoCommit();
            conn.setAutoCommit(false);
            try {

                try (Statement st = conn.createStatement()) {
                    prepareTemporaryPhoneTables(st);
                }

                try (java.sql.PreparedStatement ins = conn.prepareStatement(
                        INSERT_PREPROC_PHONES_SQL)) {
                    for (Long p : phones12) {
                        ins.setLong(1, p);
                        ins.addBatch();
                    }
                    ins.executeBatch();
                }

                try (java.sql.Statement stAnalyze = conn.createStatement()) {
                    stAnalyze.execute(ANALYZE_PREPROC_PHONES_SQL);
                }

                try (java.sql.Statement st2 = conn.createStatement();
                     java.sql.ResultSet rs = st2.executeQuery(SELECT_PREPROC_PHONES_SQL)) {
                    while (rs.next()) {
                        long phone12 = formatPhoneWithPrefix(rs.getLong(COLUMN_PHONE));
                        long phone10 = formatPhoneWithoutPrefix(phone12);
                        result.put(phone10, telcelOperatorCodes.contains(rs.getString(COLUMN_OPERATOR)));
                    }
                }
                // Use LEFT JOIN to exclude portability (faster than NOT IN); LATERAL for one series match per phone
                try (Statement st = conn.createStatement();
                     ResultSet rs = st.executeQuery(SELECT_PREPROC_PHONES_WITH_JOIN_TABLES_SQL)) {
                    while (rs.next()) {
                        long phone12 = formatPhoneWithPrefix(rs.getLong(COLUMN_PHONE));
                        long phone10 = formatPhoneWithoutPrefix(phone12);
                        var op = rs.getString(COLUMN_OPERATOR);
                        var vOp = rs.getString(COLUMN_VIRTUAL_OPERATOR);
                        var isTelcel = (op != null && telcelOperatorCodes.contains(op))
                                || (vOp != null && telcelOperatorCodes.contains(vOp));
                        result.put(phone10, isTelcel);
                    }
                }
                conn.commit();
            } catch (Exception e) {
                conn.rollback();
                throw e;
            } finally {
                conn.setAutoCommit(prevAutoCommit);
            }
            return null;
        });

        phoneNumbers.stream().filter(p -> !result.containsKey(p)).forEach(p -> result.put(p, false));
        return result;
    }

    private static void prepareTemporaryPhoneTables(Statement st) throws SQLException {
        st.execute(CREATE_PREPROC_PHONES_SQL);
        st.execute(DELETE_PREPROC_PHONES_SQL);
    }

    private Optional<Boolean> checkPortability(long phoneNumber) {
        try {
            var rows = jdbc.queryForList(SELECT_PORTABILITY_SQL, String.class, phoneNumber);
            if (!rows.isEmpty()) {
                return java.util.Optional.of(telcelOperatorCodes.contains(rows.get(0)));
            }
        } catch (Exception e) {
            log.warn("⚠️ Portability lookup failed for {}: {}", phoneNumber, e.getMessage());
        }
        return java.util.Optional.empty();
    }

    private boolean checkSeries(long phoneNumber) {
        try {
            var rows = jdbc.queryForList(SELECT_SERIES_QUERY, phoneNumber, phoneNumber);
            if (!rows.isEmpty()) {
                var op = (String) rows.get(0).get(COLUMN_OPERATOR);
                var virtualOp = (String) rows.get(0).get(COLUMN_VIRTUAL_OPERATOR);
                return telcelOperatorCodes.contains(op) || telcelOperatorCodes.contains(virtualOp);
            }
        } catch (Exception e) {
            log.warn("⚠️ Series lookup failed for {}: {}", phoneNumber, e.getMessage());
        }
        return false;
    }

    private Long formatPhoneWithPrefix(long phoneNumber){
        return  phoneNumber < 10_000_000_000L ? 52_0000_000_000L + phoneNumber : phoneNumber;
    }

    private Long formatPhoneWithoutPrefix(long phoneNumber){
        return  phoneNumber > 52_0000_000_000L ? phoneNumber  - 52_0000_000_000L: phoneNumber;
    }
}
