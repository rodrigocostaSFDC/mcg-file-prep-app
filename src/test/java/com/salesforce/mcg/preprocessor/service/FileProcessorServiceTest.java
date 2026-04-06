package com.salesforce.mcg.preprocessor.service;

import com.opencsv.exceptions.CsvValidationException;
import com.salesforce.mcg.preprocessor.data.ShortUrlResponse;
import com.salesforce.mcg.preprocessor.util.FormatUtil;
import com.salesforce.mcg.preprocessor.util.PreprocessorBusinessClock;
import com.salesforce.mcg.preprocessor.helper.FileProcessorHelper;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.when;

/**
 * Unit tests for {@link FileProcessorService}.
 */
@ExtendWith(MockitoExtension.class)
class FileProcessorServiceTest {

    @Mock
    private TelcelCheckService telcelCheckService;
    @Mock
    private ShortCodeBalancer shortCodeBalancer;
    @Mock
    private ShortUrlService shortUrlService;

    private FileProcessorService fileProcessorService;

    @BeforeEach
    void setUp() {
        fileProcessorService = new FileProcessorService(
                telcelCheckService,
                shortCodeBalancer,
                shortUrlService,
                new PreprocessorBusinessClock("America/Mexico_City"),
                100,
                true,
                1,
                "local-test",
                "telmex",
                "01/01/2001 00:00:00.");
        ;
    }

    @Test
    void buildHeaderIndex_shouldMapHeadersCaseInsensitive() {
        String[] header = {"celular", "URL", "url2", "Other"};
        Map<String, Integer> index = FileProcessorHelper.buildHeaderIndex(header);

        assertThat(index.get("CELULAR")).isEqualTo(0);
        assertThat(index.get("URL")).isEqualTo(1);
        assertThat(index.get("URL2")).isEqualTo(2);
        assertThat(index.get("OTHER")).isEqualTo(3);
    }

    @Test
    void buildHeaderIndex_shouldStripBomAndWhitespace() {
        String[] header = {"\uFEFF CELULAR ", "  URL  "};
        Map<String, Integer> index = FileProcessorHelper.buildHeaderIndex(header);

        assertThat(index.get("CELULAR")).isEqualTo(0);
        assertThat(index.get("URL")).isEqualTo(1);
    }

    @Test
    void process_shouldResolveCelularAndFallbackToTelefono() throws IOException, CsvValidationException {
        // Header with TELEFONO but no CELULAR
        String input = "TELEFONO|URL|URL2\n5512345678|https://a.com|https://b.com\n";
        ByteArrayInputStream in = new ByteArrayInputStream(input.getBytes(StandardCharsets.UTF_8));
        ByteArrayOutputStream out = new ByteArrayOutputStream();

        when(telcelCheckService.isTelcelBatch(anyList())).thenReturn(java.util.Map.of(5512345678L, false));
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
        assertThat(output).contains("TELEFONO|URL|URL2|ISTELCEL|SHORTCODE|ERROR");
        assertThat(output).contains("false|89992|");
    }

    @Test
    void process_shouldThrowWhenNeitherCelularNorTelefonoPresent() throws IOException, CsvValidationException {
        String input = "FOO|BAR|BAZ\n1|2|3\n";
        ByteArrayInputStream in = new ByteArrayInputStream(input.getBytes(StandardCharsets.UTF_8));
        ByteArrayOutputStream out = new ByteArrayOutputStream();

        assertThatThrownBy(() -> fileProcessorService.process(in, out, "run1"))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("CELULAR")
                .hasMessageContaining("TELEFONO");
    }

}
