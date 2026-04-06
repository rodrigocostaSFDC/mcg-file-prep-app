package com.salesforce.mcg.preprocessor.service;

import org.junit.jupiter.api.Test;
import org.springframework.test.util.ReflectionTestUtils;

import static org.assertj.core.api.Assertions.assertThat;

class ShortCodeBalancerTest {

    @Test
    void assignShortCode_usesTelcelAndRoundRobinForNonTelcel() {
        ShortCodeBalancer balancer = new ShortCodeBalancer();
        ReflectionTestUtils.setField(balancer, "telcelShortCode", "89992");
        ReflectionTestUtils.setField(balancer, "nonTelcelShortCode1", "35000");
        ReflectionTestUtils.setField(balancer, "nonTelcelShortCode2", "90120");

        assertThat(balancer.assignShortCode(true)).isEqualTo("89992");
        assertThat(balancer.assignShortCode(false)).isEqualTo("90120");
        assertThat(balancer.assignShortCode(false)).isEqualTo("35000");
        assertThat(balancer.assignShortCode(false)).isEqualTo("90120");
    }
}
