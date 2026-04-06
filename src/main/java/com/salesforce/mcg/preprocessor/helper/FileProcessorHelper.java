package com.salesforce.mcg.preprocessor.helper;

import com.opencsv.*;
import com.salesforce.mcg.preprocessor.data.ColumnMapping;
import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;

import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

@Slf4j
public class FileProcessorHelper {

    // -------------------------------------------------------------------------
    // File Constants
    // -------------------------------------------------------------------------

    public static final char DELIMITER = '|';

    // -------------------------------------------------------------------------
    // Header Constants
    // -------------------------------------------------------------------------

    public static final String PHONE_HEADER = "CELULAR";
    public static final String PHONE_HEADER_FALLBACK = "TELEFONO";
    public static final List<String> URL_HEADERS = List.of("URL", "URL2");
    public static final List<String> API_KEY_HEADERS = List.of("TCODE");
    public static final List<String> TEMPLATE_NAME_HEADERS = List.of("TNAME");
    public static final List<String> SUBSCRIBER_KEY_HEADERS = List.of("SUBSCRIBER_KEY", "MOBILE_USER_ID");

    // -------------------------------------------------------------------------
    // General Helpers
    // -------------------------------------------------------------------------

    public static String nullToEmpty(String s) {
        return s != null ? s.strip() : "";
    }

    public static String[] appendColumns(String[] original, String... extra) {
        String[] result = Arrays.copyOf(original, original.length + extra.length);
        System.arraycopy(extra, 0, result, original.length, extra.length);
        return result;
    }

    public static String safeGet(String[] row, int index) {
        return (row != null && index >= 0 && index < row.length) ? row[index] : "";
    }

    public static long parsePhone(String raw) {
        if (raw == null || raw.isBlank()) return 0L;
        String digits = raw.replaceAll("[^0-9]", "");
        if (digits.isEmpty()) return 0L;
        try {
            return Long.parseLong(digits);
        } catch (NumberFormatException e) {
            log.warn("⚠️ Could not parse phone number '{}' — defaulting to 0", raw);
            return 0L;
        }
    }

    /**
     * Builds a case-insensitive header name → column index map from the header row.
     * BOM characters and surrounding whitespace are stripped.
     */
    public static Map<String, Integer> buildHeaderIndex(String[] header) {
        Map<String, Integer> map = new HashMap<>();
        for (int i = 0; i < header.length; i++) {
            String name = header[i]
                    .replace("\uFEFF", "") // strip UTF-8 BOM if present
                    .strip()
                    .toUpperCase();
            map.put(name, i);
        }
        return map;
    }

    public static String getRequestId(
            String uniqueEnvId,
            String fileRequestId,
            int rowIndex,
            int colIdx){
        return "%s_%s_%s_%s".formatted(uniqueEnvId, fileRequestId, rowIndex, colIdx);
    }

    public static int resolvePhoneColumn(Map<String, Integer> index) {
        if (index.containsKey(PHONE_HEADER)) return index.get(PHONE_HEADER);
        if (index.containsKey(PHONE_HEADER_FALLBACK)) return index.get(PHONE_HEADER_FALLBACK);
        var errorMessage = "❌Required column '%s' (or fallback '%s') not found in header. Available columns: %s"
                .formatted(PHONE_HEADER, PHONE_HEADER_FALLBACK, index.keySet());
        throw new IllegalArgumentException(errorMessage);
    }

    // -------------------------------------------------------------------------
    // File Reader and Writer methods
    // -------------------------------------------------------------------------

    public static CSVReader getReaderForInputStream(@NonNull InputStream inputStream){
        var parser = new RFC4180ParserBuilder().withSeparator(DELIMITER).build();
        return new CSVReaderBuilder(
                new InputStreamReader(inputStream, StandardCharsets.UTF_8))
                .withCSVParser(parser).build();
    }

    public static ICSVWriter getWriterForOutputStream(@NonNull OutputStream outputStream){
        return new CSVWriterBuilder(
                new OutputStreamWriter(outputStream, StandardCharsets.UTF_8))
                .withSeparator(DELIMITER).build();
    }

    // -------------------------------------------------------------------------
    // Column Mapping methods
    // -------------------------------------------------------------------------

    public static ColumnMapping getColumnMapping(Map<String, Integer> headerIndex){
        int phoneColIdx = resolvePhoneColumn(headerIndex);
        List<Integer> urlColIdxs = URL_HEADERS.stream()
                .filter(headerIndex::containsKey)
                .map(headerIndex::get)
                .toList();
        int apiKeyColIdx = resolveColumn(headerIndex, API_KEY_HEADERS);
        int templateNameColIdx = resolveColumn(headerIndex, TEMPLATE_NAME_HEADERS);
        int subscriberKeyColIdx = resolveColumn(headerIndex, SUBSCRIBER_KEY_HEADERS);
        return new ColumnMapping(phoneColIdx, urlColIdxs, apiKeyColIdx, templateNameColIdx, subscriberKeyColIdx);
    }

    public static int resolveColumn(Map<String, Integer> index, List<String> headers) {
        return headers.stream()
                .filter(index::containsKey)
                .map(index::get)
                .findFirst()
                .orElse(-1);
    }

    public static String removeUnusedColumns(String message, int maxColumns) {
        if (message == null || message.isEmpty()) {
            return "";
        }
        if (message.length() <= maxColumns) {
            return message;
        }
        return message.substring(0, maxColumns);
    }

}
