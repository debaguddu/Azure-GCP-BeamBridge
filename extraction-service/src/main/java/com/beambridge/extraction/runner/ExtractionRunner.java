package com.beambridge.extraction.runner;

import com.beambridge.extraction.service.KeysetExtractionService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.CommandLineRunner;
import org.springframework.stereotype.Component;

import java.util.Arrays;
import java.util.List;

/**
 * Startup runner to trigger the extraction pipeline when invoked as a GKE Job / Batch Pod.
 */
@Component
public class ExtractionRunner implements CommandLineRunner {

    private static final Logger log = LoggerFactory.getLogger(ExtractionRunner.class);
    private final KeysetExtractionService extractionService;

    @Value("${pipeline.extraction.run-on-startup:true}")
    private boolean runOnStartup;

    @Value("${pipeline.extraction.target-tables:}")
    private String configuredTargetTables;

    public ExtractionRunner(KeysetExtractionService extractionService) {
        this.extractionService = extractionService;
    }

    @Override
    public void run(String... args) {
        if (!runOnStartup) {
            log.info("Automatic startup extraction is disabled (pipeline.extraction.run-on-startup=false). Service listening for API triggers.");
            return;
        }

        // Check if specific tables were supplied via CLI arguments (e.g. --table=orders or --tables=categories,customers)
        String cliTable = null;
        for (String arg : args) {
            if (arg.startsWith("--table=")) {
                cliTable = arg.substring("--table=".length());
            }
        }

        if (cliTable != null && !cliTable.trim().isEmpty()) {
            log.info("Target table specified via command line: '{}'", cliTable);
            extractionService.extractTable(cliTable.trim());
        } else if (configuredTargetTables != null && !configuredTargetTables.trim().isEmpty()) {
            List<String> tables = Arrays.stream(configuredTargetTables.split(","))
                    .map(String::trim)
                    .filter(s -> !s.isEmpty())
                    .toList();
            log.info("Processing tables configured in properties: {}", tables);
            for (String table : tables) {
                extractionService.extractTable(table);
            }
        } else {
            // Process all active tables from PostgreSQL pipeline_control
            extractionService.extractAllActiveTables();
        }

        log.info("ExtractionRunner execution cycle completed.");
    }
}
