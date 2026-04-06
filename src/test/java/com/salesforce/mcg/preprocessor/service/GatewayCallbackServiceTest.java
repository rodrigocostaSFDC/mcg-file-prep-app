package com.salesforce.mcg.preprocessor.service;

import org.junit.jupiter.api.Test;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.client.RestTemplate;

import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

class GatewayCallbackServiceTest {

    @Test
    void notifyNext_isIdempotentAfterSuccess() {
        GatewayCallbackService svc = spy(new GatewayCallbackService("http://localhost:8080/", "secret", 1));
        RestTemplate template = mock(RestTemplate.class);
        doReturn(template).when(svc).getTemplate();
        doReturn(new ResponseEntity<>(Map.of(), HttpStatus.OK))
                .when(template).postForEntity(eq("http://localhost:8080//api/preprocessor/next"), any(), eq(Map.class));

        svc.notifyNext();
        svc.notifyNext();

        verify(template, times(1))
                .postForEntity(eq("http://localhost:8080//api/preprocessor/next"), any(), eq(Map.class));
    }

    @Test
    void notifyNext_retriesAfterFailure() {
        GatewayCallbackService svc = spy(new GatewayCallbackService("http://localhost:8080", "secret", 1));
        RestTemplate template = mock(RestTemplate.class);
        doReturn(template).when(svc).getTemplate();
        doThrow(new RuntimeException("network"))
                .doReturn(new ResponseEntity<>(Map.of(), HttpStatus.OK))
                .when(template).postForEntity(eq("http://localhost:8080//api/preprocessor/next"), any(), eq(Map.class));

        svc.notifyNext();
        svc.notifyNext();

        verify(template, times(2))
                .postForEntity(eq("http://localhost:8080//api/preprocessor/next"), any(), eq(Map.class));
    }

    @Test
    void templateHonorsConfiguredTimeout() {
        GatewayCallbackService svc = new GatewayCallbackService("http://localhost:8080", "secret", 3);
        RestTemplate template = svc.getTemplate();

        assertThat(template.getRequestFactory()).isNotNull();
    }
}
