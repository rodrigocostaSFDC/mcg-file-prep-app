package com.salesforce.mcg.preprocessor.helper;

import com.opencsv.CSVReader;
import com.opencsv.ICSVWriter;
import com.salesforce.mcg.preprocessor.data.ColumnMapping;
import org.junit.jupiter.api.Test;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.nio.charset.StandardCharsets;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class FileProcessorHelperTest {

    @Test
    void basicHelpers_coverCommonCases() {
        assertThat(FileProcessorHelper.nullToEmpty(null)).isEmpty();
        assertThat(FileProcessorHelper.nullToEmpty("  x ")).isEqualTo("x");
        assertThat(FileProcessorHelper.safeGet(new String[]{"a"}, 0)).isEqualTo("a");
        assertThat(FileProcessorHelper.safeGet(new String[]{"a"}, 1)).isEmpty();
        assertThat(FileProcessorHelper.safeGet(null, 0)).isEmpty();
        assertThat(FileProcessorHelper.appendColumns(new String[]{"a"}, "b", "c"))
                .containsExactly("a", "b", "c");
    }

    @Test
    void parsePhone_handlesFormattingAndOverflow() {
        assertThat(FileProcessorHelper.parsePhone("52 55-1234-5678")).isEqualTo(525512345678L);
        assertThat(FileProcessorHelper.parsePhone("abc")).isZero();
        assertThat(FileProcessorHelper.parsePhone("")).isZero();
        assertThat(FileProcessorHelper.parsePhone("99999999999999999999999")).isZero();
    }

    @Test
    void requestIdHelper_work() {
        assertThat(FileProcessorHelper.getRequestId("dyno1", "fileA", 3, 1))
                .isEqualTo("dyno1_fileA_3_1");
    }

    @Test
    void headerAndMappingResolution_workAndValidateRequiredPhone() {
        Map<String, Integer> index = FileProcessorHelper.buildHeaderIndex(
                new String[]{"\uFEFF celular ", "url", "URL2", "TCODE", "TNAME", "SUBSCRIBER_KEY"});
        assertThat(index.get("CELULAR")).isEqualTo(0);

        ColumnMapping mapping = FileProcessorHelper.getColumnMapping(index);
        assertThat(mapping.phoneColIdx()).isEqualTo(0);
        assertThat(mapping.urlColIdxs()).containsExactly(1, 2);
        assertThat(mapping.apiKeyColIdx()).isEqualTo(3);
        assertThat(mapping.templateNameColIdx()).isEqualTo(4);
        assertThat(mapping.subscriberKeyColIdx()).isEqualTo(5);

        assertThat(FileProcessorHelper.resolveColumn(index, java.util.List.of("MISSING"))).isEqualTo(-1);
        assertThatThrownBy(() -> FileProcessorHelper.resolvePhoneColumn(Map.of("URL", 1)))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("CELULAR")
                .hasMessageContaining("TELEFONO");
    }

    @Test
    void readerWriterAndMessageTrimming_work() throws Exception {
        String input = "A|B\n1|2\n";
        CSVReader reader = FileProcessorHelper.getReaderForInputStream(
                new ByteArrayInputStream(input.getBytes(StandardCharsets.UTF_8)));
        assertThat(reader.readNext()).containsExactly("A", "B");
        assertThat(reader.readNext()).containsExactly("1", "2");
        reader.close();

        ByteArrayOutputStream out = new ByteArrayOutputStream();
        ICSVWriter writer = FileProcessorHelper.getWriterForOutputStream(out);
        writer.writeNext(new String[]{"x", "y"}, false);
        writer.close();
        assertThat(out.toString(StandardCharsets.UTF_8)).contains("x|y");

        assertThat(FileProcessorHelper.removeUnusedColumns(null, 3)).isEmpty();
        assertThat(FileProcessorHelper.removeUnusedColumns("abcd", 3)).isEqualTo("abc");
        assertThat(FileProcessorHelper.removeUnusedColumns("ab", 3)).isEqualTo("ab");
    }
}
