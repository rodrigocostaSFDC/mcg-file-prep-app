package com.salesforce.mcg.preprocessor.data;

import java.io.Serializable;

/**
 * Response received from {@code POST /api/shorturl/batch} on the short URL service.
 */
public record ShortUrlResponse(
        String id,
        String requestId,
        String subscriberId,
        String originalUrl,
        String shortUrl,
        String error
) implements Serializable {
}
