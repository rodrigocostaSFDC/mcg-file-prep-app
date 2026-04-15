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
import com.salesforce.mcg.preprocessor.data.ShortUrlRequest;
import com.salesforce.mcg.preprocessor.data.ShortUrlResponse;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicLong;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static com.salesforce.mcg.preprocessor.helper.FileProcessorHelper.*;

@Service
@Slf4j
public class FileProcessorService {

    private static final Pattern URL_PATTERN =
            Pattern.compile("(https?://[^\\s\"'<>]+)", Pattern.CASE_INSENSITIVE);

    private static final DateTimeFormatter TRANSACTION_DATE_FMT =
            DateTimeFormatter.ofPattern("dd/MM/yyyy HH:mm:ss");

    public static final int MAX_ERROR_COLUMN_LENGTH = 400;

    public static final String ERROR_CELULAR_REQUIRED_FOR_SHORT_URL =
            "CELULAR is required for short URL enrichment";

    static final String PHONE_HEADER = "CELULAR";
    static final String PHONE_HEADER_TELEFONO = "TELEFONO";
    static final List<String> URL_HEADERS = List.of("URL", "URL2");
    static final List<String> API_KEY_HEADERS = List.of("CAMPAIGNCODE", "TCODE");
    static final List<String> TEMPLATE_NAME_HEADERS = List.of("TNAME");
    static final List<String> EMAIL_HEADERS = List.of("EMAIL");
    static final List<String> TCODE_HEADERS = List.of("TCODE");

    private final TelcelCheckService telcelCheckService;
    private final ShortCodeBalancer shortCodeBalancer;
    private final ShortUrlService shortUrlService;

    private final int chunkSize;
    private final boolean includeHeader;
    private final int parallelBatches;
    private final String uniqueEnvId;
    private final String company;
    private final Instant transactionDate;

    public FileProcessorService(
            TelcelCheckService telcelCheckService,
            ShortCodeBalancer shortCodeBalancer,
            ShortUrlService shortUrlService,
            @Value("${preprocessor.chunk-size:5000}") int chunkSize,
            @Value("${preprocessor.output.include-header:true}") boolean includeHeader,
            @Value("${shorturl.parallel-batches:4}") int parallelBatches,
            @Value("#{environment['DYNO'] ?: 'local-' + T(java.util.UUID).randomUUID().toString()}") String uniqueEnvId,
            @Value("${app.company}") String company,
            @Value("${app.transactionDate}") String transactionDate) {
        this.telcelCheckService = telcelCheckService;
        this.shortCodeBalancer = shortCodeBalancer;
        this.shortUrlService = shortUrlService;
        this.chunkSize = chunkSize;
        this.includeHeader = includeHeader;
        this.parallelBatches = parallelBatches;
        this.uniqueEnvId = uniqueEnvId;
        this.company = company;
        this.transactionDate = LocalDateTime.parse(transactionDate, TRANSACTION_DATE_FMT)
                .toInstant(ZoneOffset.UTC);
    }

    @TrackElapsed("FileProcessorService.process")
    public long process(InputStream inputStream,
                        OutputStream outputStream,
                        String fileRequestId)
            throws IOException, CsvValidationException {

        AtomicLong totalRows = new AtomicLong(0);
        AtomicLong errorRows = new AtomicLong(0);

        try (CSVReader reader = getReaderForInputStream(inputStream);
             ICSVWriter writer = getWriterForOutputStream(outputStream)) {

            String[] header = reader.readNext();
            if (header == null) {
                log.warn("⚠️ Input file is empty — nothing to process");
                return 0L;
            }

            Map<String, Integer> headerIndex = buildHeaderIndex(header);

            int celularColIdx = resolveRequiredColumn(headerIndex, PHONE_HEADER);
            int telefonoColIdx = resolveRequiredColumn(headerIndex, PHONE_HEADER_TELEFONO);
            List<Integer> urlColIdxs = URL_HEADERS.stream()
                    .filter(headerIndex::containsKey)
                    .map(headerIndex::get)
                    .toList();
            int apiKeyColIdx = resolveColumn(headerIndex, API_KEY_HEADERS);
            int templateNameColIdx = resolveColumn(headerIndex, TEMPLATE_NAME_HEADERS);
            int emailColIdx = resolveColumn(headerIndex, EMAIL_HEADERS);
            int tcodeColIdx = resolveColumn(headerIndex, TCODE_HEADERS);

            if (includeHeader) {
                writer.writeNext(appendColumns(header, "ISTELCEL", "SHORTCODE", "ERROR"), false);
            }

            List<String[]> chunk = new ArrayList<>(chunkSize);
            String[] row;
            long chunkIndex = 1;

            while ((row = reader.readNext()) != null) {
                chunk.add(row);
                if (chunk.size() >= chunkSize) {
                    long errors = processChunk(chunk, writer, fileRequestId, chunkIndex,
                            totalRows, celularColIdx, telefonoColIdx, urlColIdxs,
                            apiKeyColIdx, templateNameColIdx, emailColIdx, tcodeColIdx);
                    errorRows.addAndGet(errors);
                    chunkIndex++;
                    chunk.clear();
                }
            }
            if (!chunk.isEmpty()) {
                long errors = processChunk(chunk, writer, fileRequestId, chunkIndex,
                        totalRows, celularColIdx, telefonoColIdx, urlColIdxs,
                        apiKeyColIdx, templateNameColIdx, emailColIdx, tcodeColIdx);
                errorRows.addAndGet(errors);
            }
        }

        return errorRows.get();
    }

    // -------------------------------------------------------------------------
    // Chunk pipeline
    // -------------------------------------------------------------------------

    private long processChunk(List<String[]> chunk, ICSVWriter writer,
                              String fileRequestId, long chunkIndex,
                              AtomicLong totalRows,
                              int celularColIdx, int telefonoColIdx,
                              List<Integer> urlColIdxs,
                              int apiKeyColIdx, int templateNameColIdx,
                              int emailColIdx, int tcodeColIdx) {

        int size = chunk.size();
        totalRows.addAndGet(size);

        long t0 = System.currentTimeMillis();
        List<Long> phones = new ArrayList<>(size);
        for (String[] r : chunk) {
            phones.add(parsePhone(safeGet(r, celularColIdx)));
        }
        Map<Long, Boolean> telcelMap = telcelCheckService.isTelcelBatch(phones);
        long telcelMs = System.currentTimeMillis() - t0;

        List<ProcessedLineHolder> lines = new ArrayList<>(size);
        for (int i = 0; i < size; i++) {
            String[] r = chunk.get(i);
            long phone = phones.get(i);
            boolean isTelcel = telcelMap.getOrDefault(telcelResultMapKey(phone), false);
            int rowNum = (int) ((chunkIndex - 1) * this.chunkSize + i);

            String celular = digitsOnly(safeGet(r, celularColIdx));
            String telefono = digitsOnly(safeGet(r, telefonoColIdx).replaceFirst("^52", ""));
            String email = safeGet(r, emailColIdx);
            String apiKey = safeGet(r, apiKeyColIdx);
            String templateName = safeGet(r, templateNameColIdx);
            String tcode = safeGet(r, tcodeColIdx);
            String shortCode = shortCodeBalancer.assignShortCode(isTelcel);

            lines.add(new ProcessedLineHolder(
                    rowNum, r, isTelcel, shortCode,
                    celular, telefono, email, apiKey, templateName, tcode,
                    false, null));
        }

        long t1 = System.currentTimeMillis();
        replaceUrlsInChunk(lines, urlColIdxs, fileRequestId);
        long shortUrlMs = System.currentTimeMillis() - t1;

        log.info("⚙️ Chunk #{}: {} rows (total: {}) — Telcel {}ms, ShortUrl {}ms",
                chunkIndex, size, totalRows.get(), telcelMs, shortUrlMs);

        long errors = 0;
        for (ProcessedLineHolder line : lines) {
            writer.writeNext(appendColumns(
                    line.originalColumns,
                    String.valueOf(line.telcel),
                    line.shortCode,
                    line.hasError ? removeUnusedColumns(line.errorMessage, MAX_ERROR_COLUMN_LENGTH) : ""), false);
            if (line.hasError) errors++;
        }

        return errors;
    }

    // -------------------------------------------------------------------------
    // URL replacement within a chunk
    // -------------------------------------------------------------------------

    private void replaceUrlsInChunk(List<ProcessedLineHolder> lines,
                                    List<Integer> urlColIdxs,
                                    String fileRequestId) {
        if (urlColIdxs.isEmpty()) return;

        record UrlRef(int lineIndex, int colIdx, String originalUrl) {}

        List<ShortUrlRequest> requests = new ArrayList<>();
        List<UrlRef> refs = new ArrayList<>();

        for (int li = 0; li < lines.size(); li++) {
            ProcessedLineHolder line = lines.get(li);

            boolean hasUrlToShorten = false;
            for (int colIdx : urlColIdxs) {
                String value = safeGet(line.originalColumns, colIdx);
                if (value != null && !value.isBlank()) { hasUrlToShorten = true; break; }
            }
            if (!hasUrlToShorten) continue;

            if (line.celular.isBlank()) {
                line.hasError = true;
                line.errorMessage = ERROR_CELULAR_REQUIRED_FOR_SHORT_URL;
                continue;
            }

            for (int colIdx : urlColIdxs) {
                String value = safeGet(line.originalColumns, colIdx);
                if (value == null || value.isBlank()) continue;

                Matcher matcher = URL_PATTERN.matcher(value);
                String originalUrl = matcher.find() ? matcher.group(1) : value.strip();

                var requestId = getRequestId(uniqueEnvId, fileRequestId, line.rowIndex, colIdx);

                requests.add(new ShortUrlRequest(
                        null,
                        originalUrl,
                        requestId,
                        line.celular,
                        line.telefono,
                        line.email,
                        company,
                        "SMS",
                        line.shortCode,
                        line.apiKey,
                        line.templateName,
                        fileRequestId,
                        transactionDate,
                        line.tcode));
                refs.add(new UrlRef(li, colIdx, originalUrl));
            }
        }

        if (requests.isEmpty()) return;

        List<ShortUrlResponse> responses;
        try {
            if (parallelBatches > 1 && requests.size() >= parallelBatches * 100) {
                responses = callShortUrlParallel(requests);
            } else {
                responses = shortUrlService.shortUrlBatch(requests);
            }
        } catch (Exception e) {
            log.error("❌ Short URL batch failed for chunk — keeping original URLs: {}", e.getMessage());
            for (UrlRef ref : refs) {
                ProcessedLineHolder line = lines.get(ref.lineIndex);
                line.originalColumns[ref.colIdx] = ref.originalUrl;
            }
            lines.forEach(l -> {
                l.hasError = true;
                l.errorMessage = "Short URL error: " + e.getMessage();
            });
            return;
        }

        for (int i = 0; i < refs.size(); i++) {
            UrlRef ref = refs.get(i);
            ProcessedLineHolder line = lines.get(ref.lineIndex);
            ShortUrlResponse resp = responses.get(i);

            if (resp.error() != null) {
                line.originalColumns[ref.colIdx] = ref.originalUrl;
                line.hasError = true;
                line.errorMessage = resp.error();
                continue;
            }

            String[] cols = line.originalColumns;
            String original = safeGet(cols, ref.colIdx);
            Matcher m = URL_PATTERN.matcher(original);
            if (m.find()) {
                cols[ref.colIdx] = m.replaceFirst(Matcher.quoteReplacement(resp.shortUrl()));
            } else {
                cols[ref.colIdx] = resp.shortUrl();
            }
        }
    }

    private List<ShortUrlResponse> callShortUrlParallel(List<ShortUrlRequest> requests) throws Exception {
        int n = Math.min(parallelBatches, Math.max(1, requests.size() / 100));
        if (n <= 1) {
            return shortUrlService.shortUrlBatch(requests);
        }
        int size = requests.size();
        int partSize = (size + n - 1) / n;
        List<List<ShortUrlRequest>> parts = new ArrayList<>(n);
        for (int i = 0; i < n; i++) {
            int from = i * partSize;
            int to = Math.min(from + partSize, size);
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

    // -------------------------------------------------------------------------
    // Helpers
    // -------------------------------------------------------------------------

    private static int resolveRequiredColumn(Map<String, Integer> index, String header) {
        if (index.containsKey(header)) return index.get(header);
        throw new IllegalArgumentException(
                "Required column '" + header + "' not found in header. Available: " + index.keySet());
    }

    /**
     * Lightweight mutable holder for a single row during chunk processing.
     * Avoids Lombok builder overhead for high-throughput chunk pipelines.
     */
    static final class ProcessedLineHolder {
        final int rowIndex;
        final String[] originalColumns;
        final boolean telcel;
        final String shortCode;
        final String celular;
        final String telefono;
        final String email;
        final String apiKey;
        final String templateName;
        final String tcode;
        boolean hasError;
        String errorMessage;

        ProcessedLineHolder(int rowIndex, String[] originalColumns,
                            boolean telcel, String shortCode,
                            String celular, String telefono,
                            String email, String apiKey,
                            String templateName, String tcode,
                            boolean hasError, String errorMessage) {
            this.rowIndex = rowIndex;
            this.originalColumns = originalColumns;
            this.telcel = telcel;
            this.shortCode = shortCode;
            this.celular = celular;
            this.telefono = telefono;
            this.email = email;
            this.apiKey = apiKey;
            this.templateName = templateName;
            this.tcode = tcode;
            this.hasError = hasError;
            this.errorMessage = errorMessage;
        }
    }
}
