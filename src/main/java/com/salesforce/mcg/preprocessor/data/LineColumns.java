package com.salesforce.mcg.preprocessor.data;

import org.apache.logging.log4j.util.Strings;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;

public class LineColumns {

    public static final String COL_CELULAR = "CELULAR";
    public static final String COL_TELEFONO = "TELEFONO";
    public static final String COL_MOBILE_USER_ID = "MOBILE_USER_ID";
    public static final String COL_SUBSCRIBER_KEY = "SUBSCRIBER_KEY";
    public static final String COL_EMAIL = "EMAIL";
    public static final String COL_CAMPAIGN_CODE = "CAMPAIGNCODE";
    public static final String COL_TCODE = "TCODE";
    public static final String COL_TNAME = "TNAME";
    public static final String COL_SHORTCODE = "SHORTCODE";
    public static final String COL_ISTELCEL = "ISTELCEL";
    public static final String COL_URL = "URL";
    public static final String COL_URL2 = "URL2";

    private static final List<String> PHONE_HEADERS = List.of(COL_TELEFONO);
    private static final List<String> MOBILE_HEADERS = List.of(COL_CELULAR);
    private static final List<String> URL_HEADERS = List.of(COL_URL, COL_URL2);
    private static final List<String> EMAIL_HEADERS = List.of(COL_EMAIL);
    private static final List<String> API_KEY_HEADERS = List.of(COL_CAMPAIGN_CODE);
    private static final List<String> TEMPLATE_HEADERS = List.of(COL_TNAME);
    private static final List<String> TCODE_HEADERS = List.of(COL_TCODE);

    private final String[] row;
    private final int rowIndex;
    private final int phoneIndex;
    private final int mobileIndex;
    private final int emailIndex;
    private final int apiKeyIndex;
    private final int templateIndex;
    private final int tcodeIndex;
    private final List<Integer> urlIndexes;

    public LineColumns(Map<String, Integer> headerIndex, String[] row, int rowIndex) {
        this.row = Objects.requireNonNull(row, "row");
        this.rowIndex = rowIndex;
        this.phoneIndex = resolveFirst(headerIndex, PHONE_HEADERS);
        this.mobileIndex = resolveFirst(headerIndex, MOBILE_HEADERS);
        this.emailIndex = resolveFirst(headerIndex, EMAIL_HEADERS);
        this.apiKeyIndex = resolveFirst(headerIndex, API_KEY_HEADERS);
        this.templateIndex = resolveFirst(headerIndex, TEMPLATE_HEADERS);
        this.tcodeIndex = resolveFirst(headerIndex, TCODE_HEADERS);
        this.urlIndexes = URL_HEADERS.stream()
                .filter(headerIndex::containsKey)
                .map(headerIndex::get)
                .toList();
    }

    public static void validateHeader(Map<String, Integer> headerIndex) {
        boolean hasPhone = PHONE_HEADERS.stream().anyMatch(headerIndex::containsKey);
        if (!hasPhone) {
            throw new IllegalArgumentException(
                    "Required column 'CELULAR' (or fallback 'TELEFONO') not found in header. Available columns: "
                            + headerIndex.keySet());
        }
    }

    public String getPhoneNumber() {
        return safeGet(phoneIndex).replaceAll("^52", Strings.EMPTY);
    }

    public String getMobileNumber() {
        return safeGet(mobileIndex);
    }

    public String getEmail() {
        return safeGet(emailIndex);
    }

    public String getApiKey() {
        return safeGet(apiKeyIndex);
    }

    public String getTemplateName() {
        return safeGet(templateIndex);
    }

    public String getTcode() {
        return safeGet(tcodeIndex);
    }

    public List<UrlField> collectUrlsToShorten() {
        List<UrlField> fields = new ArrayList<>();
        for (int urlIndex : urlIndexes) {
            String current = safeGet(urlIndex);
            if (current.isBlank()) {
                continue;
            }
            fields.add(new UrlField(urlIndex, current));
        }
        return fields;
    }

    public void setUrlValue(int columnIndex, String value) {
        if (columnIndex < 0 || columnIndex >= row.length) {
            return;
        }
        row[columnIndex] = value;
    }

    private static int resolveFirst(Map<String, Integer> headerIndex, List<String> candidates) {
        return candidates.stream()
                .filter(headerIndex::containsKey)
                .map(headerIndex::get)
                .findFirst()
                .orElse(-1);
    }

    private String safeGet(int index) {
        return (index >= 0 && index < row.length && row[index] != null) ? row[index] : "";
    }

    public record UrlField(int columnIndex, String originalUrl) {}
}
