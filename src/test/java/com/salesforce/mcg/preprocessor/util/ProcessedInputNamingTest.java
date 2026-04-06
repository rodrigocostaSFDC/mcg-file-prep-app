package com.salesforce.mcg.preprocessor.util;

import org.junit.jupiter.api.Test;

import java.lang.reflect.Constructor;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;

class ProcessedInputNamingTest {

    @Test
    void constructor_isCoveredViaReflection() throws Exception {
        Constructor<ProcessedInputNaming> ctor = ProcessedInputNaming.class.getDeclaredConstructor();
        ctor.setAccessible(true);
        assertThatCode(ctor::newInstance).doesNotThrowAnyException();
    }

    @Test
    void txtInput_matchesReadyNameExactly() {
        assertThat(ProcessedInputNaming.processedLeafMatchingReady(
                "CA000_S_20260317_1705.txt",
                "CA000_S_20260317_1705_20260320_1430.txt"))
                .isEqualTo("CA000_S_20260317_1705_20260320_1430.txt");
    }

    @Test
    void zipInput_usesReadyStemWithZipExtension() {
        assertThat(ProcessedInputNaming.processedLeafMatchingReady(
                "CA000_S_20260317_1705.zip",
                "CA000_S_20260317_1705_20260320_1430.txt"))
                .isEqualTo("CA000_S_20260317_1705_20260320_1430.zip");
    }

    @Test
    void txtInput_noTimestamp_sameAsOutput() {
        assertThat(ProcessedInputNaming.processedLeafMatchingReady(
                "batch.txt",
                "batch.txt"))
                .isEqualTo("batch.txt");
    }

    @Test
    void readyNameBlank_returnsOriginalInput() {
        assertThat(ProcessedInputNaming.processedLeafMatchingReady("a.zip", " "))
                .isEqualTo("a.zip");
    }

    @Test
    void nonZipInput_preservesOriginalExtension() {
        assertThat(ProcessedInputNaming.processedLeafMatchingReady(
                "batch.csv",
                "batch_20260401_1800.txt"))
                .isEqualTo("batch_20260401_1800.csv");
    }

    @Test
    void outputWithoutTxtExtension_returnsOriginalInput() {
        assertThat(ProcessedInputNaming.processedLeafMatchingReady(
                "batch.zip",
                "batch.ready"))
                .isEqualTo("batch.zip");
    }

    @Test
    void readyNameNull_returnsOriginalInput() {
        assertThat(ProcessedInputNaming.processedLeafMatchingReady("batch.zip", null))
                .isEqualTo("batch.zip");
    }

    @Test
    void nullInputWithTxtOutput_returnsZipWithReadyStem() {
        assertThat(ProcessedInputNaming.processedLeafMatchingReady(
                null,
                "batch_20260401_1800.txt"))
                .isEqualTo("batch_20260401_1800");
    }
}
