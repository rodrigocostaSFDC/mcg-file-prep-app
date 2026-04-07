package com.salesforce.mcg.preprocessor.helper;

import com.opencsv.CSVReader;
import com.opencsv.ICSVWriter;
import org.junit.jupiter.api.Test;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.nio.charset.StandardCharsets;

import static org.assertj.core.api.Assertions.assertThat;

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
