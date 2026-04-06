package com.salesforce.mcg.preprocessor.time;

import com.salesforce.mcg.preprocessor.util.PreprocessorBusinessClock;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

class PreprocessorBusinessClockTest {

    @Test
    void buildsClockWithConfiguredZoneAndReturnsNow() {
        PreprocessorBusinessClock clock = new PreprocessorBusinessClock("America/Mexico_City");

        assertThat(clock.getZone().getId()).isEqualTo("America/Mexico_City");
        assertThat(clock.now()).isNotNull();
    }
}
