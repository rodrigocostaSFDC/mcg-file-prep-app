package com.salesforce.mcg.preprocessor.util;

import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;

public class FormatUtil {

    public static Instant toInstant(String dateStr) {
        DateTimeFormatter formatter = DateTimeFormatter.ofPattern("dd/MM/yyyy HH:mm:ss");

        LocalDateTime localDateTime = LocalDateTime.parse(dateStr, formatter);

        // IMPORTANT: define the timezone
        ZoneId zone = ZoneId.systemDefault(); // or ZoneId.of("America/Sao_Paulo")

        return localDateTime.atZone(zone).toInstant();
    }
}
