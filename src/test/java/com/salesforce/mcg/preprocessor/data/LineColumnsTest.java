package com.salesforce.mcg.preprocessor.data;

import org.junit.jupiter.api.Test;

import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

class LineColumnsTest {

    @Test
    void collectUrlsToShorten_shouldWorkByColumnAndNotByContent() {
        String[] row = {"525512345678", "5512345678", "not-a-url"};
        Map<String, Integer> headerIndex = Map.of(
                "CELULAR", 0,
                "TELEFONO", 1,
                "URL", 2
        );

        LineColumns columns = new LineColumns(headerIndex, row, 7);
        var fields = columns.collectUrlsToShorten();

        assertThat(fields).hasSize(1);
        assertThat(fields.get(0).originalUrl()).isEqualTo("not-a-url");

        columns.setUrlValue(fields.get(0).columnIndex(), "https://short/a");
        assertThat(row[2]).isEqualTo("https://short/a");
    }
}
