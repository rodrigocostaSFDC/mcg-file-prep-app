package com.salesforce.mcg.preprocessor.util;

import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;

public class FormatUtil {

    private static final DateTimeFormatter PARSER =
            DateTimeFormatter.ofPattern("dd/MM/yyyy HH:mm:ss");

    /** Parses a UTC-formatted date string ({@code dd/MM/yyyy HH:mm:ss}) into an {@link Instant}. */
    public static Instant toInstant(String dateStr) {
        return LocalDateTime.parse(dateStr, PARSER)
                .atOffset(ZoneOffset.UTC)
                .toInstant();
    }
}
