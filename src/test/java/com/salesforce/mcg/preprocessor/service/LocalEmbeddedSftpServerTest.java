package com.salesforce.mcg.preprocessor.service;

import com.salesforce.mcg.preprocessor.properties.SftpPreprocessorProperties;
import com.salesforce.mcg.preprocessor.properties.SftpServerProperties;
import com.salesforce.mcg.preprocessor.util.SftpPropertyContext;
import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.Test;

import java.io.IOException;

import static org.assertj.core.api.Assertions.assertThatCode;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

class LocalEmbeddedSftpServerTest {

    @Test
    void startsAndStopsEmbeddedServer() throws Exception {
        String company = "test_" + System.nanoTime();
        SftpServerProperties props = new SftpServerProperties(
                company,
                "127.0.0.1",
                0,
                "prepuser",
                "pass",
                "",
                "",
                "",
                true,
                "/",
                "/ready_files",
                "*",
                20000,
                3,
                30000);

        SftpPropertyContext context = mock(SftpPropertyContext.class);
        when(context.getPropertiesForActiveCompany()).thenReturn(props);
        SftpPreprocessorProperties all = new SftpPreprocessorProperties(props, props);

        try {
            LocalEmbeddedSftpServer server = new LocalEmbeddedSftpServer(context, all);
            assertThatCode(server::destroy).doesNotThrowAnyException();
            assertThatCode(server::destroy).doesNotThrowAnyException();
        } catch (IOException ioe) {
            String msg = ioe.getMessage() == null ? "" : ioe.getMessage();
            Assumptions.assumeFalse(msg.contains("Operation not permitted"),
                    "Sandbox blocks opening listening sockets");
            throw ioe;
        }
    }
}
