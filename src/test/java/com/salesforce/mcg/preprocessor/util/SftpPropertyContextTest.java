package com.salesforce.mcg.preprocessor.util;

import com.salesforce.mcg.preprocessor.properties.SftpPreprocessorProperties;
import com.salesforce.mcg.preprocessor.properties.SftpServerProperties;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

class SftpPropertyContextTest {

    @Test
    void returnsTelmexWhenCompanyIsTelmex() {
        SftpServerProperties telmex = new SftpServerProperties("telmex", "h1", 22, "u", "p", "", "", "", true, "/in", "/out", "*", 20000, 3, 30000);
        SftpServerProperties telnor = new SftpServerProperties("telnor", "h2", 23, "u2", "p2", "", "", "", true, "/in2", "/out2", "*", 20000, 3, 30000);
        SftpPropertyContext ctx = new SftpPropertyContext(new SftpPreprocessorProperties(telmex, telnor), "telmex");

        assertThat(ctx.getPropertiesForActiveCompany()).isEqualTo(telmex);
    }

    @Test
    void returnsTelnorWhenCompanyIsNotTelmex() {
        SftpServerProperties telmex = new SftpServerProperties("telmex", "h1", 22, "u", "p", "", "", "", true, "/in", "/out", "*", 20000, 3, 30000);
        SftpServerProperties telnor = new SftpServerProperties("telnor", "h2", 23, "u2", "p2", "", "", "", true, "/in2", "/out2", "*", 20000, 3, 30000);
        SftpPropertyContext ctx = new SftpPropertyContext(new SftpPreprocessorProperties(telmex, telnor), "other");

        assertThat(ctx.getPropertiesForActiveCompany()).isEqualTo(telnor);
    }
}
