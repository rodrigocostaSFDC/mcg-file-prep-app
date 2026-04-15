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

import com.opencsv.CSVReader;
import com.opencsv.ICSVWriter;
import com.opencsv.exceptions.CsvValidationException;
import com.salesforce.mcg.preprocessor.aspect.TrackElapsed;
import com.salesforce.mcg.preprocessor.data.*;
import com.salesforce.mcg.preprocessor.util.FormatUtil;
import com.salesforce.mcg.preprocessor.util.PreprocessorBusinessClock;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicLong;

import static com.salesforce.mcg.preprocessor.helper.FileProcessorHelper.*;

/**
 * Chunked-streaming preprocessor for large customer/template CSV files.
 *
 * <h2>Why chunked streaming?</h2>
 * Files can contain 5–15 million rows. Loading the full file into a
 * {@code List<String[]>} would require several GB of heap and cause OOM errors.
 * Instead, this service:
 * <ol>
 *   <li>Reads {@link #chunkSize} rows at a time from the input {@link InputStream}.
 *   <li>Resolves Telcel status for the chunk (batch JDBC query).
 *   <li>Assigns short codes per row.
 *   <li>Generates short URLs for all URLs in the chunk (batch JDBC insert or HTTP call).
 *   <li>Writes the enriched chunk rows directly to an {@link OutputStream} provided by
 *       the caller — which is typically a {@code PipedOutputStream} wired to an SFTP
 *       upload, so the output never accumulates in RAM.
 *   <li>Discards the chunk and reads the next one.
 * </ol>
 *
 * <p>At steady state, only {@code chunkSize} rows (+ one chunk of short URL requests)
 * live in the heap at once.  For a 100-byte row at chunk size 5 000, that is ~500 KB
 * per chunk — negligible on any Heroku dyno size.
 *
 * <h2>Pipeline per chunk</h2>
 * <ol>
 *   <li>Read up to {@code chunkSize} rows from {@link CSVReader}.
 *   <li>Batch Telcel lookup: {@link TelcelCheckService#isTelcelBatch}.
 *   <li>Short code assignment: {@link ShortCodeBalancer#assignShortCode}.
 *   <li>Short URL generation: {@link ShortUrlService#shortUrlBatch} (direct) or
 *   <li>Write enriched rows to output {@link ICSVWriter} (plain {@code .txt} bytes when used from the launcher).
 * </ol>
 *
 * <h2>Short URL mode</h2>
 * Controlled by {@code shorturl.mode} (env {@code SHORTURL_MODE}):
 * <ul>
 *   <li>{@code direct} (default) — JDBC insert to {@code shorturl_sch.short_url}
 *   <li>{@code http} — HTTP POST to {@code mcg-short-url-app /api/shorturl/batch}
 * </ul>
 *
 * <h2>Error handling</h2>
 * Per-row errors are non-fatal: the row is written with an {@code ERROR} column populated
 * (capped at 400 characters for downstream systems) and processing continues.
 * The caller receives the total error count for reporting.
 */
@Service
@Slf4j
public class FileProcessorService {

    /** Max length for the appended {@code ERROR} column text (inclusive). */
    public static final int MAX_ERROR_COLUMN_LENGTH = 400;
    public static final String SMS = "SMS";

    /** Written to the {@code ERROR} column when a row has URLs to shorten but {@code CELULAR} is blank. */
    public static final String ERROR_CELULAR_REQUIRED_FOR_SHORT_URL =
            "CELULAR is required for short URL enrichment";

    private final TelcelCheckService telcelCheckService;
    private final ShortCodeBalancer shortCodeBalancer;
    private final ShortUrlService shortUrlService;
    private final PreprocessorBusinessClock businessClock;

    /** How many CSV rows to hold in memory at once. 5 000 is safe for any dyno size. */
    private final int chunkSize;
    /** Include header in output file **/
    private final boolean includeHeader;
    /** Split ShortUrl batch into N parallel batches (default 1). Use 4 for ~4x speedup; requires DB pool size >= N. */
    private final int parallelBatches;
    /** Identify the unique execution id for heroku or local **/
    private final String uniqueEnvId;
    /** Company name **/
    private final String company;
    /** Transaction date **/
    private final Instant transactionDate;

    public FileProcessorService(
            TelcelCheckService telcelCheckService,
            ShortCodeBalancer shortCodeBalancer,
            ShortUrlService shortUrlService,
            PreprocessorBusinessClock businessClock,
            @Value("${preprocessor.chunk-size:5000}") int chunkSize,
            @Value("${preprocessor.output.include-header:true}") boolean includeHeader,
            @Value("${shorturl.parallel-batches:4}") int parallelBatches,
            @Value("#{environment['DYNO'] ?: 'local-' + T(java.util.UUID).randomUUID().toString()}") String uniqueEnvId,
            @Value("${app.company}") String company,
            @Value("${app.transactionDate}") String transactionDate){
        this.telcelCheckService = telcelCheckService;
        this.shortCodeBalancer = shortCodeBalancer;
        this.shortUrlService = shortUrlService;
        this.businessClock = businessClock;
        this.chunkSize = chunkSize;
        this.includeHeader = includeHeader;
        this.parallelBatches = parallelBatches;
        this.uniqueEnvId = uniqueEnvId;
        this.company = company;
        this.transactionDate = FormatUtil.toInstant(transactionDate);
    }

    /**
     * Processes the pipe-delimited file in a chunked-streaming fashion.
     *
     * <p>Column positions are resolved once from the header row by name. Required: {@code CELULAR}
     * (12-digit mobile {@code 52…}), {@code TELEFONO} (10-digit national, or 12-digit with leading {@code 52}
     * stripped once), {@code URL}, {@code URL2}. Optional: {@code TCODE} → api_key, {@code TNAME} → template_name.
     * Short-URL batch maps {@code CELULAR} → {@code mobile_number}, {@code TELEFONO} → {@code phone_number}.
     *
     * <h3>How URL/URL2 map to short-URL batch results</h3>
     * For each chunk, the preprocessor builds two parallel lists:
     * <ul>
     *   <li>{@code requests} — one {@link ShortUrlRequest} per URL cell (row × URL column), in row-major order
     *       (row 0 URL, row 0 URL2, row 1 URL, …).</li>
     *   <li>{@code refs} — same length; entry {@code i} is {@code (lineIndex, colIdx)} for that request.</li>
     * </ul>
     * {@link ShortUrlService#shortUrlBatch} / HTTP batch return {@link ShortUrlResponse} list in the
     * <b>same order</b> as {@code requests}.  Parallel sub-batches are merged in submission order, so
     * index {@code i} always pairs {@code responses.get(i)} with {@code refs.get(i)} for in-place replacement
     * in {@code originalColumns[colIdx]}.
     *
     * @param inputStream   pipe-delimited file stream from SFTP download
     * @param outputStream  destination stream (e.g. PipedOutputStream → SFTP upload)
     * @param fileRequestId correlation ID for this processing run (used in short URL tracking)
     * @return total number of rows that had a processing error (written with an error note)
     * @throws IOException            on stream I/O failure
     * @throws CsvValidationException on malformed input
     */
    @TrackElapsed("FileProcessorService.process")
    public long process(InputStream inputStream,
                        OutputStream outputStream,
                        String fileRequestId)
            throws IOException, CsvValidationException {

        AtomicLong totalRows = new AtomicLong(0);
        AtomicLong errorRows = new AtomicLong(0);

        FileChunk fileChunk = null;

        log.info("ℹ️ Processor thread started for runId={}", fileRequestId);

        try (CSVReader reader = getReaderForInputStream(inputStream);
             ICSVWriter writer = getWriterForOutputStream(outputStream)) {

            // --- Header row: resolve column indices by name ---
            String[] header = reader.readNext();
            if (header == null) {
                log.warn("⚠️ Input file is empty — nothing to process");
                return 0L;
            }
            log.info("ℹ️ Header read OK — {} columns", header.length);

            Map<String, Integer> headerIndex = buildHeaderIndex(header);
            LineColumns.validateHeader(headerIndex);
            if (includeHeader) {
                writer.writeNext(appendColumns(header, "ISTELCEL", "SHORTCODE", "ERROR"), false);
            }

            // --- Chunked read-process-write loop ---
            List<String[]> chunk = new ArrayList<>(chunkSize);
            String[] row;
            long chunkIndex = 1;
            long skippedRows = 0;

            while (true) {
                try {
                    row = reader.readNext();
                } catch (CsvValidationException e) {
                    skippedRows++;
                    errorRows.incrementAndGet();
                    log.warn("⚠️ Skipping malformed CSV row (line ~{}): {}",
                            totalRows.get() + chunk.size() + skippedRows + 1, e.getMessage());
                    continue;
                }
                if (row == null) break;

                chunk.add(row);
                fileChunk = new FileChunk(chunk, writer, fileRequestId, chunkIndex, totalRows);

                if (chunk.size() >= chunkSize) {
                    log.info("ℹ️ Chunk #{} read complete ({} rows) — starting Telcel+ShortURL…",
                            chunkIndex, chunk.size());
                    long errors = processChunk(fileChunk, headerIndex);
                    errorRows.addAndGet(errors);
                    chunkIndex++;
                    chunk.clear();
                }
            }
            if (skippedRows > 0) {
                log.warn("⚠️ Total malformed CSV rows skipped: {}", skippedRows);
            }
            // flush remaining rows
            if (Objects.nonNull(fileChunk) && !fileChunk.isEmpty()) {
                long errors = processChunk(fileChunk, headerIndex);
                errorRows.addAndGet(errors);
            }
        }

        return errorRows.get();
    }

    // -------------------------------------------------------------------------
    // Chunk pipeline
    // -------------------------------------------------------------------------

    /**
     * Runs the full pipeline for one chunk and writes enriched rows to {@code writer}.
     *
     * @return number of rows in this chunk that had a recoverable error
     */
    private long processChunk(FileChunk fileChunk, Map<String, Integer> headerIndex) {

        int size = fileChunk.chunk().size();
        fileChunk.totalRows().addAndGet(size);

        // Step 1: resolve row data by header name and prepare all phones from this chunk
        List<LineColumns> chunkLines = new ArrayList<>(size);
        List<Long> phones = new ArrayList<>(size);
        for (int i = 0; i < size; i++) {
            String[] row = fileChunk.chunk().get(i);
            int rowNum = Long.valueOf(((fileChunk.chunkIndex() - 1) * this.chunkSize + i)).intValue();
            LineColumns lineColumns = new LineColumns(headerIndex, row, rowNum);
            chunkLines.add(lineColumns);
            phones.add(parsePhone(lineColumns.getCelularDigits()));
        }

        // Step 2: Batch Telcel lookup for this chunk
        log.info("ℹ️ Chunk #{}: Telcel batch lookup for {} phones…", fileChunk.chunkIndex(), phones.size());
        Map<Long, Boolean> telcelMap = telcelCheckService.isTelcelBatch(phones);
        log.info("ℹ️ Chunk #{}: Telcel lookup done", fileChunk.chunkIndex());

        // Step 3: Build ProcessedLine entries — one per row.
        List<ProcessedLine> lines = new ArrayList<>(size);
        for (int i = 0; i < size; i++) {

            String[] row = fileChunk.chunk().get(i);
            LineColumns lineColumns = chunkLines.get(i);
            var phone = phones.get(i);
            var isTelcel = telcelMap.getOrDefault(telcelResultMapKey(phone), false);
            var rowNum = Long.valueOf(((fileChunk.chunkIndex() - 1) * this.chunkSize + i)).intValue();
            var phoneNumber = lineColumns.getPhoneNumber();
            var mobileNumber = lineColumns.getMobileNumber();
            var email = lineColumns.getEmail();
            var apiKey = lineColumns.getApiKey();
            var shortCode = shortCodeBalancer.assignShortCode(isTelcel);
            var tcode = lineColumns.getTcode();
            var templateName = lineColumns.getTemplateName();

            lines.add(ProcessedLine.builder()
                    .rowIndex(rowNum)
                    .originalColumns(row)
                    .lineColumns(lineColumns)
                    .shortCode(shortCode)
                    .apiKey(apiKey)
                    .mobileNumber(mobileNumber)
                    .phoneNumber(phoneNumber)
                    .email(email)
                    .company(company)
                    .messageType(SMS)
                    .templateName(templateName)
                    .transactionDate(transactionDate)
                    .tcode(tcode)
                    .telcel(isTelcel)
                    .build());
        }

        // Step 3: Shorten URLs found in each URL column, in-place in originalColumns
        log.info("ℹ️ Chunk #{}: ShortURL batch for {} lines…", fileChunk.chunkIndex(), lines.size());
        replaceUrlsInChunk(lines, fileChunk.fileRequestId());
        log.info("⚙️ Chunk #{}: {} rows (total: {})",
                fileChunk.chunkIndex(), size, fileChunk.totalRows().get());

        // Step 4: Write enriched rows to the output stream
        long errors = 0;
        for (ProcessedLine line : lines) {
            fileChunk.writer().writeNext(appendColumns(
                    line.getOriginalColumns(),
                    String.valueOf(line.isTelcel()),
                    line.getShortCode(),
                    line.isHasError() ? removeUnusedColumns(line.getErrorMessage(), MAX_ERROR_COLUMN_LENGTH) : ""), false);
            if (line.isHasError()) errors++;
        }

        return errors;
    }

    // -------------------------------------------------------------------------
    // URL replacement within a chunk
    // -------------------------------------------------------------------------

    /**
     * For each URL column index, collects all non-blank values across the chunk,
     * shortens them in a single batch call, then writes the short URLs back into
     * {@code line.originalColumns[colIdx]} in-place.
     * Blank {@code CELULAR} with URLs to shorten: no short-URL request; row error
     * {@link #ERROR_CELULAR_REQUIRED_FOR_SHORT_URL}. Blank {@code TELEFONO} is allowed.
     * <p>
     * Ordering: {@code requests} and {@code refs} are built in nested (line, URL column) order.
     * The short-URL service returns {@link ShortUrlResponse}s in the same order as {@code requests},
     * so {@code responses.get(i)} always corresponds to {@code refs.get(i)}.
     */
    private void replaceUrlsInChunk(
            List<ProcessedLine> lines,
            String fileRequestId) {
        record UrlRef(int lineIndex, int columnIndex, String originalUrl) {}

        List<ShortUrlRequest> requests = new ArrayList<>();
        List<UrlRef> refs = new ArrayList<>();

        for (int li = 0; li < lines.size(); li++) {
            var line = lines.get(li);
            var urlFields = line.getLineColumns().collectUrlsToShorten();
            if (urlFields.isEmpty()) {
                continue;
            }
            if (line.getLineColumns().getCelularDigits().isBlank()) {
                line.setHasError(true);
                line.setErrorMessage(ERROR_CELULAR_REQUIRED_FOR_SHORT_URL);
                continue;
            }
            for (LineColumns.UrlField urlField : urlFields) {
                var requestId = getRequestId(uniqueEnvId, fileRequestId, line.getRowIndex(), urlField.columnIndex());
                requests.add(new ShortUrlRequest(
                        line.getId(),
                        urlField.originalUrl(),
                        requestId,
                        line.getMobileNumber(),
                        line.getPhoneNumber(),
                        line.getEmail(),
                        line.getCompany(),
                        line.getMessageType(),
                        line.getShortCode(),
                        line.getApiKey(),
                        line.getTemplateName(),
                        line.getTransactionId(),
                        transactionDate,
                        line.getTcode()));
                refs.add(new UrlRef(li, urlField.columnIndex(), urlField.originalUrl()));
            }
        }

        if (requests.isEmpty()) {
            return;
        }

        List<ShortUrlResponse> responses;
        try {
            if (parallelBatches > 1 && requests.size() >= parallelBatches * 100) {
                responses = callShortUrlParallel(requests);
            } else {
                responses = shortUrlService.shortUrlBatch(requests);
            }
        } catch (Exception e) {
            log.error("Short URL batch failed for chunk — keeping original URLs: {}", e.getMessage());
            for (UrlRef ref : refs) {
                lines.get(ref.lineIndex()).getLineColumns().setUrlValue(ref.columnIndex(), ref.originalUrl());
            }
            lines.forEach(l -> {
                l.setHasError(true);
                l.setErrorMessage("Short URL error: " + e.getMessage());
            });
            return;
        }

        // Write short URLs back into the original row using placeholders
        for (int i = 0; i < refs.size(); i++) {
            UrlRef ref = refs.get(i);
            ProcessedLine line = lines.get(ref.lineIndex());
            ShortUrlResponse resp = responses.get(i);

            if (resp.error() != null) {
                // Collision — surface as a row error, leave original URL intact
                line.getLineColumns().setUrlValue(ref.columnIndex(), ref.originalUrl());
                line.setHasError(true);
                line.setErrorMessage(resp.error());
                continue;
            }

            line.getLineColumns().setUrlValue(ref.columnIndex(), resp.shortUrl());
        }
    }

    /**
     * Splits requests into N parts, runs shortUrlBatch on each in parallel, merges results.
     */
    private List<ShortUrlResponse> callShortUrlParallel(List<ShortUrlRequest> requests) throws Exception {
        int n = Math.min(parallelBatches, Math.max(1, requests.size() / 100));
        if (n <= 1) {
            return shortUrlService.shortUrlBatch(requests);
        }
        int size = requests.size();
        int chunkSize = (size + n - 1) / n;
        List<List<ShortUrlRequest>> parts = new ArrayList<>(n);
        for (int i = 0; i < n; i++) {
            int from = i * chunkSize;
            int to = Math.min(from + chunkSize, size);
            if (from < to) parts.add(requests.subList(from, to));
        }
        ExecutorService pool = Executors.newFixedThreadPool(parts.size());
        try {
            List<Future<List<ShortUrlResponse>>> futures = parts.stream()
                    .map(sub -> pool.submit(() -> shortUrlService.shortUrlBatch(sub)))
                    .toList();
            List<ShortUrlResponse> merged = new ArrayList<>(size);
            for (Future<List<ShortUrlResponse>> f : futures) {
                merged.addAll(f.get());
            }
            return merged;
        } finally {
            pool.shutdown();
        }
    }

}
