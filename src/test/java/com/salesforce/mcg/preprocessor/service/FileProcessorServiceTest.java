package com.salesforce.mcg.preprocessor.service;

import com.opencsv.exceptions.CsvValidationException;
import com.salesforce.mcg.preprocessor.data.ShortUrlRequest;
import com.salesforce.mcg.preprocessor.data.ShortUrlResponse;
import com.salesforce.mcg.preprocessor.helper.FileProcessorHelper;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.lang.reflect.Method;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

/**
 * Unit tests for {@link FileProcessorService}. Input rows require both {@code CELULAR} and {@code TELEFONO}.
 */
@ExtendWith(MockitoExtension.class)
class FileProcessorServiceTest {

    /** 12-digit mobile + 10-digit national (same subscriber) for mocks. */
    private static final String C = "525512345678";
    private static final String T = "5512345678";
    private static final String CT = "CELULAR|TELEFONO|";

    @Mock
    private TelcelCheckService telcelCheckService;
    @Mock
    private ShortCodeBalancer shortCodeBalancer;
    @Mock
    private ShortUrlService shortUrlService;

    private FileProcessorService fileProcessorService;

    @BeforeEach
    void setUp() {
        fileProcessorService = buildService(100, true, 1);
    }

    @Test
    void buildHeaderIndex_shouldMapHeadersCaseInsensitive() {
        String[] header = {"celular", "telefono", "URL", "url2", "Other"};
        Map<String, Integer> index = FileProcessorHelper.buildHeaderIndex(header);

        assertThat(index.get("CELULAR")).isEqualTo(0);
        assertThat(index.get("TELEFONO")).isEqualTo(1);
        assertThat(index.get("URL")).isEqualTo(2);
        assertThat(index.get("URL2")).isEqualTo(3);
        assertThat(index.get("OTHER")).isEqualTo(4);
    }

    @Test
    void buildHeaderIndex_shouldStripBomAndWhitespace() {
        String[] header = {"\uFEFF CELULAR ", " TELEFONO ", "  URL  "};
        Map<String, Integer> index = FileProcessorHelper.buildHeaderIndex(header);

        assertThat(index.get("CELULAR")).isEqualTo(0);
        assertThat(index.get("TELEFONO")).isEqualTo(1);
        assertThat(index.get("URL")).isEqualTo(2);
    }

    @Test
    void process_shouldAcceptCelularAndTelefono() throws IOException, CsvValidationException {
        String input = CT + "URL|URL2\n" + C + "|" + T + "|https://a.com|https://b.com\n";
        ByteArrayInputStream in = new ByteArrayInputStream(input.getBytes(StandardCharsets.UTF_8));
        ByteArrayOutputStream out = new ByteArrayOutputStream();

        when(telcelCheckService.isTelcelBatch(anyList())).thenReturn(Map.of(5512345678L, false));
        when(shortCodeBalancer.assignShortCode(false)).thenReturn("89992");
        when(shortUrlService.shortUrlBatch(anyList())).thenAnswer(inv -> {
            List<?> reqs = inv.getArgument(0);
            return reqs.stream()
                    .map(r -> new ShortUrlResponse(null, null, null, null, "https://short.example/abc123", null))
                    .toList();
        });

        long errors = fileProcessorService.process(in, out, "run1");

        assertThat(errors).isEqualTo(0);
        String output = out.toString(StandardCharsets.UTF_8);
        assertThat(output).contains("CELULAR|TELEFONO|URL|URL2|ISTELCEL|SHORTCODE|ERROR");
        assertThat(output).contains("false|89992|");
        verify(telcelCheckService).isTelcelBatch(List.of(525512345678L));
    }

    @Test
    void process_shouldThrowWhenCelularMissing() {
        String input = "TELEFONO|URL\n" + T + "|https://a.com\n";
        assertThatThrownBy(() -> fileProcessorService.process(
                new ByteArrayInputStream(input.getBytes(StandardCharsets.UTF_8)),
                new ByteArrayOutputStream(),
                "run1"))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("CELULAR");
    }

    @Test
    void process_shouldThrowWhenTelefonoMissing() {
        String input = "CELULAR|URL\n" + C + "|https://a.com\n";
        assertThatThrownBy(() -> fileProcessorService.process(
                new ByteArrayInputStream(input.getBytes(StandardCharsets.UTF_8)),
                new ByteArrayOutputStream(),
                "run1"))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("TELEFONO");
    }

    @Test
    void process_shouldReturnZeroWhenInputIsEmpty() throws IOException, CsvValidationException {
        ByteArrayInputStream in = new ByteArrayInputStream(new byte[0]);
        ByteArrayOutputStream out = new ByteArrayOutputStream();

        long errors = fileProcessorService.process(in, out, "run-empty");

        assertThat(errors).isZero();
        assertThat(out.toString(StandardCharsets.UTF_8)).isEmpty();
        verifyNoInteractions(telcelCheckService, shortCodeBalancer, shortUrlService);
    }

    @Test
    void process_shouldSkipHeaderWhenConfigured() throws IOException, CsvValidationException {
        fileProcessorService = buildService(100, false, 1);
        String input = CT + "URL\n" + C + "|" + T + "| \n";
        ByteArrayInputStream in = new ByteArrayInputStream(input.getBytes(StandardCharsets.UTF_8));
        ByteArrayOutputStream out = new ByteArrayOutputStream();

        when(telcelCheckService.isTelcelBatch(anyList())).thenReturn(Map.of(5512345678L, false));
        when(shortCodeBalancer.assignShortCode(false)).thenReturn("89992");

        long errors = fileProcessorService.process(in, out, "run-no-header");

        assertThat(errors).isZero();
        String normalized = normalizeOutput(out);
        assertThat(normalized).doesNotContain("ISTELCEL|SHORTCODE|ERROR");
        assertThat(normalized).startsWith(C + "|" + T + "| |false|89992|");
        verify(shortUrlService, never()).shortUrlBatch(anyList());
    }

    @Test
    void process_shouldMarkErrorWhenShortUrlBatchFailsAndTrimErrorColumn() throws IOException, CsvValidationException {
        String longMessage = "x".repeat(500);
        String input = CT + "URL\n" + C + "|" + T + "|https://a.com\n";
        ByteArrayInputStream in = new ByteArrayInputStream(input.getBytes(StandardCharsets.UTF_8));
        ByteArrayOutputStream out = new ByteArrayOutputStream();

        when(telcelCheckService.isTelcelBatch(anyList())).thenReturn(Map.of(5512345678L, false));
        when(shortCodeBalancer.assignShortCode(false)).thenReturn("89992");
        when(shortUrlService.shortUrlBatch(anyList())).thenThrow(new RuntimeException(longMessage));

        long errors = fileProcessorService.process(in, out, "run-shorturl-error");

        assertThat(errors).isEqualTo(1);
        String[] rows = normalizeOutput(out).split("\n");
        assertThat(rows).hasSize(2);
        String errorColumn = rows[1].substring(rows[1].lastIndexOf('|') + 1);
        assertThat(errorColumn).startsWith("Short URL error: ");
        assertThat(errorColumn.length()).isEqualTo(FileProcessorService.MAX_ERROR_COLUMN_LENGTH);
        assertThat(rows[1]).contains("https://a.com");
    }

    @Test
    void process_shouldMarkOnlyRowsWithCollisionError() throws IOException, CsvValidationException {
        String input = CT + "URL\n"
                + "525511111111|5511111111|https://a.com\n"
                + "525522222222|5522222222|https://b.com\n";
        ByteArrayInputStream in = new ByteArrayInputStream(input.getBytes(StandardCharsets.UTF_8));
        ByteArrayOutputStream out = new ByteArrayOutputStream();

        when(telcelCheckService.isTelcelBatch(anyList()))
                .thenReturn(Map.of(5511111111L, false, 5522222222L, true));
        when(shortCodeBalancer.assignShortCode(false)).thenReturn("89992");
        when(shortCodeBalancer.assignShortCode(true)).thenReturn("89990");
        when(shortUrlService.shortUrlBatch(anyList())).thenReturn(List.of(
                new ShortUrlResponse(null, null, null, "https://a.com", null, "collision"),
                new ShortUrlResponse(null, null, null, "https://b.com", "https://short/b", null)
        ));

        long errors = fileProcessorService.process(in, out, "run-collision");

        assertThat(errors).isEqualTo(1);
        String normalized = normalizeOutput(out);
        assertThat(normalized).contains("5511111111|https://a.com|false|89992|collision");
        assertThat(normalized).contains("5522222222|https://short/b|true|89990|");
    }

    @Test
    void process_shouldSkipShortUrlWhenNoUrlColumnExists() throws IOException, CsvValidationException {
        String input = CT + "NOMBRE\n" + C + "|" + T + "|John\n";
        ByteArrayInputStream in = new ByteArrayInputStream(input.getBytes(StandardCharsets.UTF_8));
        ByteArrayOutputStream out = new ByteArrayOutputStream();

        when(telcelCheckService.isTelcelBatch(anyList())).thenReturn(Map.of(5512345678L, true));
        when(shortCodeBalancer.assignShortCode(true)).thenReturn("89990");

        long errors = fileProcessorService.process(in, out, "run-no-url-columns");

        assertThat(errors).isZero();
        assertThat(normalizeOutput(out)).contains(T + "|John|true|89990|");
        verify(shortUrlService, never()).shortUrlBatch(anyList());
    }

    @Test
    void process_shouldErrorAndSkipShortUrlWhenCelularBlankButUrlPresent() throws IOException, CsvValidationException {
        String input = CT + "URL\n|" + T + "|https://a.com\n";
        ByteArrayInputStream in = new ByteArrayInputStream(input.getBytes(StandardCharsets.UTF_8));
        ByteArrayOutputStream out = new ByteArrayOutputStream();

        when(telcelCheckService.isTelcelBatch(anyList())).thenReturn(Map.of(5512345678L, false));
        when(shortCodeBalancer.assignShortCode(false)).thenReturn("89992");

        long errors = fileProcessorService.process(in, out, "run-empty-celular");

        assertThat(errors).isEqualTo(1);
        verify(shortUrlService, never()).shortUrlBatch(anyList());
        assertThat(normalizeOutput(out)).contains(FileProcessorService.ERROR_CELULAR_REQUIRED_FOR_SHORT_URL);
        assertThat(normalizeOutput(out)).contains("https://a.com");
    }

    @Test
    void process_shouldShortenWhenTelefonoBlankButCelularPresent() throws IOException, CsvValidationException {
        String input = CT + "URL\n" + C + "||https://a.com\n";
        ByteArrayInputStream in = new ByteArrayInputStream(input.getBytes(StandardCharsets.UTF_8));
        ByteArrayOutputStream out = new ByteArrayOutputStream();

        when(telcelCheckService.isTelcelBatch(anyList())).thenReturn(Map.of(5512345678L, false));
        when(shortCodeBalancer.assignShortCode(false)).thenReturn("89992");
        when(shortUrlService.shortUrlBatch(anyList())).thenReturn(List.of(
                new ShortUrlResponse(null, null, null, "https://a.com", "https://short/a", null)
        ));

        long errors = fileProcessorService.process(in, out, "run-empty-telefono");

        assertThat(errors).isZero();
        verify(shortUrlService, times(1)).shortUrlBatch(anyList());
        ArgumentCaptor<List<ShortUrlRequest>> captor = ArgumentCaptor.forClass(List.class);
        verify(shortUrlService).shortUrlBatch(captor.capture());
        assertThat(captor.getValue().get(0).phoneNumber()).isEmpty();
        assertThat(normalizeOutput(out)).contains("https://short/a");
    }

    @Test
    void process_shouldShortenNonBlankUrlColumnValuesEvenWhenNotHttp() throws IOException, CsvValidationException {
        String input = CT + "URL|URL2\n" + C + "|" + T + "|not-a-url| \n";
        ByteArrayInputStream in = new ByteArrayInputStream(input.getBytes(StandardCharsets.UTF_8));
        ByteArrayOutputStream out = new ByteArrayOutputStream();

        when(telcelCheckService.isTelcelBatch(anyList())).thenReturn(Map.of(5512345678L, false));
        when(shortCodeBalancer.assignShortCode(false)).thenReturn("89992");
        when(shortUrlService.shortUrlBatch(anyList())).thenReturn(List.of(
                new ShortUrlResponse(null, null, null, "not-a-url", "https://short/a", null)
        ));

        long errors = fileProcessorService.process(in, out, "run-invalid-url");

        assertThat(errors).isZero();
        assertThat(normalizeOutput(out)).contains(T + "|https://short/a| |false|89992|");
        verify(shortUrlService, times(1)).shortUrlBatch(anyList());
    }

    @Test
    void process_shouldUseParallelShortUrlBatchesForLargeRequests() throws IOException, CsvValidationException {
        fileProcessorService = buildService(500, true, 2);
        StringBuilder builder = new StringBuilder(CT + "URL|URL2\n");
        for (int i = 0; i < 100; i++) {
            builder.append(C).append("|").append(T)
                    .append("|https://example.com/a/").append(i)
                    .append("|https://example.com/b/").append(i)
                    .append('\n');
        }
        ByteArrayInputStream in = new ByteArrayInputStream(builder.toString().getBytes(StandardCharsets.UTF_8));
        ByteArrayOutputStream out = new ByteArrayOutputStream();

        when(telcelCheckService.isTelcelBatch(anyList())).thenAnswer(inv -> {
            @SuppressWarnings("unchecked")
            List<Long> phones = inv.getArgument(0);
            return phones.stream().collect(java.util.stream.Collectors.toMap(p -> p, p -> false, (a, b) -> a));
        });
        when(shortCodeBalancer.assignShortCode(false)).thenReturn("89992");
        when(shortUrlService.shortUrlBatch(anyList())).thenAnswer(inv -> {
            @SuppressWarnings("unchecked")
            List<ShortUrlRequest> reqs = inv.getArgument(0);
            List<ShortUrlResponse> responses = new ArrayList<>(reqs.size());
            for (ShortUrlRequest req : reqs) {
                responses.add(new ShortUrlResponse(req.id(), req.requestId(), null, req.originalUrl(),
                        "https://short.local/" + req.requestId(), null));
            }
            return responses;
        });

        long errors = fileProcessorService.process(in, out, "run-parallel");

        assertThat(errors).isZero();
        verify(shortUrlService, times(2)).shortUrlBatch(anyList());
        assertThat(normalizeOutput(out)).contains("https://short.local/");
    }

    @Test
    void process_shouldUseSingleShortUrlBatchWhenParallelThresholdNotMet() throws IOException, CsvValidationException {
        fileProcessorService = buildService(500, true, 4);
        String input = CT + "URL\n" + C + "|" + T + "|https://a.com\n";
        ByteArrayInputStream in = new ByteArrayInputStream(input.getBytes(StandardCharsets.UTF_8));
        ByteArrayOutputStream out = new ByteArrayOutputStream();

        when(telcelCheckService.isTelcelBatch(anyList())).thenReturn(Map.of(5512345678L, false));
        when(shortCodeBalancer.assignShortCode(false)).thenReturn("89992");
        when(shortUrlService.shortUrlBatch(anyList())).thenReturn(List.of(
                new ShortUrlResponse(null, null, null, "https://a.com", "https://short/a", null)
        ));

        long errors = fileProcessorService.process(in, out, "run-single");

        assertThat(errors).isZero();
        verify(shortUrlService, times(1)).shortUrlBatch(anyList());
        assertThat(normalizeOutput(out)).contains("https://short/a");
    }

    @Test
    void process_shouldProcessEachChunkWhenChunkSizeIsOne() throws IOException, CsvValidationException {
        fileProcessorService = buildService(1, true, 1);
        String input = CT + "URL\n"
                + "525511111111|5511111111|https://a.com\n"
                + "525522222222|5522222222|https://b.com\n";
        ByteArrayInputStream in = new ByteArrayInputStream(input.getBytes(StandardCharsets.UTF_8));
        ByteArrayOutputStream out = new ByteArrayOutputStream();

        when(telcelCheckService.isTelcelBatch(anyList()))
                .thenReturn(Map.of(5511111111L, false))
                .thenReturn(Map.of(5522222222L, false));
        when(shortCodeBalancer.assignShortCode(false)).thenReturn("89992");
        when(shortUrlService.shortUrlBatch(anyList()))
                .thenReturn(List.of(new ShortUrlResponse(null, null, null, "https://a.com", "https://short/a", null)))
                .thenReturn(List.of(new ShortUrlResponse(null, null, null, "https://b.com", "https://short/b", null)));

        long errors = fileProcessorService.process(in, out, "run-chunk-1");

        assertThat(errors).isZero();
        verify(telcelCheckService, times(2)).isTelcelBatch(anyList());
        verify(shortUrlService, times(2)).shortUrlBatch(anyList());
        assertThat(normalizeOutput(out)).contains("https://short/a").contains("https://short/b");
    }

    @Test
    void process_shortUrlRequest_passesCelularAndTelefonoToBatch() throws IOException, CsvValidationException {
        String input = CT + "URL\n" + C + "|" + T + "|https://a.com\n";
        ByteArrayInputStream in = new ByteArrayInputStream(input.getBytes(StandardCharsets.UTF_8));
        ByteArrayOutputStream out = new ByteArrayOutputStream();

        when(telcelCheckService.isTelcelBatch(anyList())).thenReturn(Map.of(5512345678L, false));
        when(shortCodeBalancer.assignShortCode(false)).thenReturn("89992");
        when(shortUrlService.shortUrlBatch(anyList())).thenReturn(List.of(
                new ShortUrlResponse(null, null, null, "https://a.com", "https://short/a", null)
        ));

        long errors = fileProcessorService.process(in, out, "run-shorturl-fields");

        assertThat(errors).isZero();
        ArgumentCaptor<List<ShortUrlRequest>> captor = ArgumentCaptor.forClass(List.class);
        verify(shortUrlService).shortUrlBatch(captor.capture());
        ShortUrlRequest request = captor.getValue().get(0);
        assertThat(request.mobileNumber()).isEqualTo("525512345678");
        assertThat(request.phoneNumber()).isEqualTo("5512345678");
    }

    @Test
    void callShortUrlParallel_shouldFallbackToSingleBatchWhenCalculatedPartitionsIsOne() throws Exception {
        fileProcessorService = buildService(100, true, 3);
        List<ShortUrlRequest> requests = List.of(new ShortUrlRequest(
                "id-1",
                "https://a.com",
                "req-1",
                "525512345678",
                "5512345678",
                "",
                "telmex",
                "SMS",
                "89992",
                "api",
                "template",
                "tx",
                java.time.Instant.now(),
                "tc"));
        List<ShortUrlResponse> expected = List.of(
                new ShortUrlResponse("id-1", "req-1", "sub-1", "https://a.com", "https://short/a", null)
        );
        when(shortUrlService.shortUrlBatch(requests)).thenReturn(expected);

        Method method = FileProcessorService.class.getDeclaredMethod("callShortUrlParallel", List.class);
        method.setAccessible(true);
        @SuppressWarnings("unchecked")
        List<ShortUrlResponse> actual = (List<ShortUrlResponse>) method.invoke(fileProcessorService, requests);

        assertThat(actual).isEqualTo(expected);
        verify(shortUrlService, times(1)).shortUrlBatch(requests);
    }

    private FileProcessorService buildService(int chunkSize, boolean includeHeader, int parallelBatches) {
        return new FileProcessorService(
                telcelCheckService,
                shortCodeBalancer,
                shortUrlService,
                chunkSize,
                includeHeader,
                parallelBatches,
                "local-test",
                "telmex",
                "01/01/2001 00:00:00");
    }

    private String normalizeOutput(ByteArrayOutputStream out) {
        return out.toString(StandardCharsets.UTF_8).replace("\r\n", "\n");
    }
}
