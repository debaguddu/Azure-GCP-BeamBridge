package com.beambridge.extraction;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.autoconfigure.jdbc.DataSourceAutoConfiguration;
import org.springframework.boot.autoconfigure.orm.jpa.HibernateJpaAutoConfiguration;

/**
 * Main application entry point for Project Kalinga: Extraction Service.
 * Coordinates multi-datasource routing between PostgreSQL Control DB and Azure SQL Source DB.
 */
@SpringBootApplication(exclude = {
        DataSourceAutoConfiguration.class,
        HibernateJpaAutoConfiguration.class
})
public class ExtractionServiceApplication {

    public static void main(String[] args) {
        SpringApplication.run(ExtractionServiceApplication.class, args);
    }
}
