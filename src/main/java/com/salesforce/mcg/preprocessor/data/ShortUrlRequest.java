package com.salesforce.mcg.preprocessor.data;

import com.fasterxml.jackson.annotation.JsonProperty;

import java.io.Serializable;
import java.sql.Timestamp;
import java.time.Instant;
import java.time.LocalDateTime;

/**
 * Payload sent to {@code POST /api/shorturl/batch} on the short URL service.
 */
public record ShortUrlRequest(
        String id,
        String originalUrl,
        String requestId,
        String subscriberKey,
        @JsonProperty("mobileNumber") String phoneNumber,
        String email,
        String company,
        String messageType,
        String shortCode,
        String apiKey,
        String templateName,
        String transactionId,
        Instant transactionDate,
        String tcode
) {}