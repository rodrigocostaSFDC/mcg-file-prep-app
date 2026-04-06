/*****************************************************************************
 * DISCLAIMER:
 * This code is provided "AS IS", without any express or implied warranties,
 * including, but not limited to, the implied warranties of merchantability,
 * fitness for a particular purpose, or non-infringement. Use of this code is
 * at your own risk. In no event shall the authors or copyright holders be
 * liable for any direct, indirect, incidental, special, exemplary, or
 * consequential damages (including, but not limited to, procurement of
 * substitute goods or services; loss of use, data, or profits; or business
 * interruption), however caused and on any theory of liability, whether in
 * contract, strict liability, or tort (including negligence or otherwise)
 * arising in any way out of the use of this code, even if advised of the
 * possibility of such damage.
 ****************************************************************************/

package com.salesforce.mcg.preprocessor.config;

import com.zaxxer.hikari.HikariDataSource;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import javax.sql.DataSource;
import java.net.URI;

import static com.salesforce.mcg.preprocessor.common.AppConstants.*;

/**
 * DataSource for JDBC (short URL batch inserts).
 * On Heroku, parses DATABASE_URL and creates a JDBC DataSource.
 * Locally, Spring Boot uses spring.datasource.url.
 */
@Configuration
@Slf4j
public class DataSourceConfig {


    @Value("${spring.datasource.hikari.maximum-pool-size:5}")
    private int maxPoolSize;

    @Bean
    @ConditionalOnProperty(name = "DATABASE_URL")
    public DataSource dataSourceFromDatabaseUrl(@Value("${DATABASE_URL}") String databaseUrl) {
        log.info("🚀 Configuring JDBC DataSource from DATABASE_URL");

        try {
            var dbUri = new URI(databaseUrl.replace("postgres://", "http://"));
            var host = dbUri.getHost();
            int port = dbUri.getPort() > 0 ? dbUri.getPort() : 5432;
            var path = dbUri.getPath();
            var database = (path != null && path.length() > 1) ? path.substring(1) : "postgres";
            var userInfo = dbUri.getUserInfo();
            if (userInfo == null) {
                throw new IllegalArgumentException("❌ DATABASE_URL missing credentials");
            }
            var credentials = userInfo.split(":", 2);
            var username = credentials[0];
            var password = credentials.length > 1 ? credentials[1] : "";
            var ssl = databaseUrl.contains(SSLMODE_REQUIRED)? "&" + SSLMODE_REQUIRED : EMPTY_STRING;
            var jdbcUrl = JDBC_CONN_STRING.formatted(host, port, database, ssl);
            var ds = new HikariDataSource();
            ds.setJdbcUrl(jdbcUrl);
            ds.setUsername(username);
            ds.setPassword(password);
            ds.setMaximumPoolSize(maxPoolSize);
            return ds;
        } catch (Exception e) {
            log.error("❌ Failed to parse DATABASE_URL and create DataSource", e);
            throw new IllegalStateException("❌ Invalid DATABASE_URL format: " + e.getMessage(), e);
        }
    }
}
