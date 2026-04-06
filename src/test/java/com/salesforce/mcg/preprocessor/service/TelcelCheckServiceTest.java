package com.salesforce.mcg.preprocessor.service;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.test.util.ReflectionTestUtils;

import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class TelcelCheckServiceTest {

    @Mock
    private JdbcTemplate jdbc;

    private TelcelCheckService service;

    @BeforeEach
    void setUp() {
        service = new TelcelCheckService(jdbc);
        ReflectionTestUtils.setField(service, "telcelOperatorCodes", List.of("006", "330", "188"));
        ReflectionTestUtils.setField(service, "forceNonTelcel", false);
    }

    @Test
    void isTelcel_returnsFalseWhenForcedNonTelcel() {
        ReflectionTestUtils.setField(service, "forceNonTelcel", true);
        assertThat(service.isTelcel(5512345678L)).isFalse();
    }

    @Test
    void isTelcel_usesPortabilityThenSeriesFallback() {
        when(jdbc.queryForList(anyString(), eq(String.class), anyLong())).thenReturn(List.of("006"));
        assertThat(service.isTelcel(5512345678L)).isTrue();

        when(jdbc.queryForList(anyString(), eq(String.class), anyLong())).thenReturn(List.of());
        when(jdbc.queryForList(anyString(), eq(5512345678L), eq(5512345678L)))
                .thenReturn(List.of(Map.of("operator", "999", "virtual_operator", "188")));
        assertThat(service.isTelcel(5512345678L)).isTrue();
    }

    @Test
    void isTelcel_handlesLookupErrorsAsFalse() {
        when(jdbc.queryForList(anyString(), eq(String.class), anyLong())).thenThrow(new RuntimeException("db"));
        when(jdbc.queryForList(anyString(), eq(5512345678L), eq(5512345678L))).thenThrow(new RuntimeException("db2"));

        assertThat(service.isTelcel(5512345678L)).isFalse();
    }

    @Test
    void isTelcelBatch_handlesEmptyAndForceNonTelcel() {
        assertThat(service.isTelcelBatch(List.of())).isEmpty();

        ReflectionTestUtils.setField(service, "forceNonTelcel", true);
        assertThat(service.isTelcelBatch(List.of(1L, 2L))).containsEntry(1L, false).containsEntry(2L, false);
    }
}
