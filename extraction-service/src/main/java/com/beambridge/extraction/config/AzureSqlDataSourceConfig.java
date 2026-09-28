package com.beambridge.extraction.config;

import com.zaxxer.hikari.HikariDataSource;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.jdbc.DataSourceProperties;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;

import javax.sql.DataSource;

/**
 * Secondary DataSource Configuration: Azure SQL Database.
 * Powers high-speed keyset pagination extraction via JDBC streaming without in-memory buffering.
 */
@Configuration
public class AzureSqlDataSourceConfig {

    @Value("${pipeline.extraction.jdbc-fetch-size:5000}")
    private int jdbcFetchSize;

    @Bean(name = "azureSqlDataSourceProperties")
    @ConfigurationProperties("spring.datasource.azuresql")
    public DataSourceProperties azureSqlDataSourceProperties() {
        return new DataSourceProperties();
    }

    @Bean(name = "azureSqlDataSource")
    @ConfigurationProperties("spring.datasource.azuresql.hikari")
    public DataSource azureSqlDataSource(
            @Qualifier("azureSqlDataSourceProperties") DataSourceProperties properties) {
        return properties.initializeDataSourceBuilder().type(HikariDataSource.class).build();
    }

    @Bean(name = "azureSqlJdbcTemplate")
    public JdbcTemplate azureSqlJdbcTemplate(
            @Qualifier("azureSqlDataSource") DataSource dataSource) {
        JdbcTemplate jdbcTemplate = new JdbcTemplate(dataSource);
        // Force the driver to stream resultsets in bounded 5,000-row chunks
        jdbcTemplate.setFetchSize(jdbcFetchSize);
        return jdbcTemplate;
    }

    @Bean(name = "azureSqlNamedJdbcTemplate")
    public NamedParameterJdbcTemplate azureSqlNamedJdbcTemplate(
            @Qualifier("azureSqlJdbcTemplate") JdbcTemplate jdbcTemplate) {
        return new NamedParameterJdbcTemplate(jdbcTemplate);
    }
}
