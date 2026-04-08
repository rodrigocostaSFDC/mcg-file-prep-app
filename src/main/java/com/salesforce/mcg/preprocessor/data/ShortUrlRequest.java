package com.salesforce.mcg.preprocessor.data;

import java.time.Instant;

/**
 * Payload for short-URL batch (direct DB or HTTP). Aligns with {@code shorturl_sch.short_url}:
 * {@code mobile_number} and {@code phone_number}.
 */
public record ShortUrlRequest(
        String id,
        String originalUrl,
        String requestId,
        /** CELULAR (12-digit {@code 52…}) → DB {@code mobile_number}. */
        String mobileNumber,
        /** TELEFONO (10-digit national; single leading {@code 52} stripped from file) → DB {@code phone_number}. */
        String phoneNumber,
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
