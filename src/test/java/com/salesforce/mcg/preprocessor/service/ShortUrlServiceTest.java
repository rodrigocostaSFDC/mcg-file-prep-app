package com.salesforce.mcg.preprocessor.service;

import com.salesforce.mcg.preprocessor.data.ShortUrlRequest;
import com.salesforce.mcg.preprocessor.data.ShortUrlResponse;
import com.salesforce.mcg.preprocessor.util.PreprocessorBusinessClock;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.test.util.ReflectionTestUtils;

import javax.sql.DataSource;
import java.lang.reflect.Method;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class ShortUrlServiceTest {

    @Mock
    private JdbcTemplate jdbc;
    @Mock
    private DataSource dataSource;
    @Mock
    private PreprocessorBusinessClock clock;

    private ShortUrlService service;

    @BeforeEach
    void setUp() {
        when(clock.getZone()).thenReturn(ZoneId.of("America/Mexico_City"));
        service = new ShortUrlService(jdbc, dataSource, clock);
        ReflectionTestUtils.setField(service, "baseDomain", "https://sho.rt");
        ReflectionTestUtils.setField(service, "maxAttempts", 3);
    }

    @Test
    void shortUrlBatch_returnsEmptyWhenNoRequests() {
        assertThat(service.shortUrlBatch(List.of())).isEmpty();
    }

    @Test
    void shortUrlBatch_insertPathBuildsResponse() {
        when(clock.now()).thenReturn(LocalDateTime.of(2026, 4, 1, 23, 0));

        ShortUrlRequest req = new ShortUrlRequest(
                null, "https://a.com", "rid1", "525512345678", "5512345678", null,
                "telmex", "SMS", "89992", "k", "tpl", "tx", Instant.now(), "tc");
        List<ShortUrlResponse> out = service.shortUrlBatch(List.of(req));

        assertThat(out).hasSize(1);
        assertThat(out.get(0).requestId()).isEqualTo("rid1");
        assertThat(out.get(0).shortUrl()).isNull();
        assertThat(out.get(0).error()).contains("collision");
    }

    @Test
    void shortUrlBatch_insertPathWithSkippedLoadsExistingShortUrl() {
        when(clock.now()).thenReturn(LocalDateTime.of(2026, 4, 1, 23, 0));
        ShortUrlRequest req = new ShortUrlRequest(
                null, "https://a.com", "rid1", "525512345678", "5512345678", null,
                "telmex", "SMS", "89992", "k", "tpl", "tx", Instant.now(), "tc");
        List<ShortUrlResponse> out = service.shortUrlBatch(List.of(req));

        assertThat(out).hasSize(1);
        assertThat(out.get(0).shortUrl()).isNull();
        assertThat(out.get(0).error()).contains("collision");
    }

    @Test
    void privateHelpers_generateExpectedFormatting() throws Exception {
        Method csvEscape = ShortUrlService.class.getDeclaredMethod("csvEscape", String.class);
        csvEscape.setAccessible(true);
        assertThat((String) csvEscape.invoke(null, "a,b")).isEqualTo("\"a,b\"");

        Method generateCode = ShortUrlService.class.getDeclaredMethod("generateCode");
        generateCode.setAccessible(true);
        String code = (String) generateCode.invoke(service);
        assertThat(code).hasSize(12);
    }
}
