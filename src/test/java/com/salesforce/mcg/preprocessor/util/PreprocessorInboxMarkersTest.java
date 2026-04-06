package com.salesforce.mcg.preprocessor.util;

import org.junit.jupiter.api.Test;

import java.lang.reflect.Constructor;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class PreprocessorInboxMarkersTest {

    @Test
    void constructor_isCoveredViaReflection() throws Exception {
        Constructor<PreprocessorInboxMarkers> ctor = PreprocessorInboxMarkers.class.getDeclaredConstructor();
        ctor.setAccessible(true);
        assertThatCode(ctor::newInstance).doesNotThrowAnyException();
    }

    @Test
    void withHasErrorsMarker_appendsSuffix() {
        assertThat(PreprocessorInboxMarkers.withHasErrorsMarker("CA1_S_20260317.zip"))
                .isEqualTo("CA1_S_20260317.zip.hasErrors");
        assertThat(PreprocessorInboxMarkers.withHasErrorsMarker("CA1_S_20260317.txt"))
                .isEqualTo("CA1_S_20260317.txt.hasErrors");
    }

    @Test
    void withHasErrorsMarker_idempotentWhenAlreadyMarked() {
        String m = "x.txt.hasErrors";
        assertThat(PreprocessorInboxMarkers.withHasErrorsMarker(m)).isEqualTo(m);
    }

    @Test
    void isHasErrorsMarked_caseInsensitive() {
        assertThat(PreprocessorInboxMarkers.isHasErrorsMarked("a.zip.hasErrors")).isTrue();
        assertThat(PreprocessorInboxMarkers.isHasErrorsMarked("a.ZIP.HASERRORS")).isTrue();
        assertThat(PreprocessorInboxMarkers.isHasErrorsMarked("a.zip")).isFalse();
        assertThat(PreprocessorInboxMarkers.isHasErrorsMarked(null)).isFalse();
    }

    @Test
    void withHasErrorsMarker_rejectsBlank() {
        assertThatThrownBy(() -> PreprocessorInboxMarkers.withHasErrorsMarker(""))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> PreprocessorInboxMarkers.withHasErrorsMarker(null))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void normalizeRequestedInputFile_extractsLeafFromPathLikeInputs() {
        assertThat(PreprocessorInboxMarkers.normalizeRequestedInputFile("test.txt"))
                .isEqualTo("test.txt");
        assertThat(PreprocessorInboxMarkers.normalizeRequestedInputFile("inbox/test.txt"))
                .isEqualTo("test.txt");
        assertThat(PreprocessorInboxMarkers.normalizeRequestedInputFile("/inbox/test.txt"))
                .isEqualTo("test.txt");
        assertThat(PreprocessorInboxMarkers.normalizeRequestedInputFile("inbox\\test.txt"))
                .isEqualTo("test.txt");
    }

    @Test
    void normalizeRequestedInputFile_returnsNullForNullOrBlank() {
        assertThat(PreprocessorInboxMarkers.normalizeRequestedInputFile(null)).isNull();
        assertThat(PreprocessorInboxMarkers.normalizeRequestedInputFile("  ")).isNull();
        assertThat(PreprocessorInboxMarkers.normalizeRequestedInputFile("/")).isNull();
        assertThat(PreprocessorInboxMarkers.normalizeRequestedInputFile("inbox/")).isNull();
    }

}
