package com.beambridge.extraction.service;

import com.beambridge.extraction.model.PipelineControl;
import com.beambridge.extraction.repository.PipelineControlRepository;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.jdbc.core.namedparam.MapSqlParameterSource;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.io.OutputStream;
import java.nio.charset.StandardCharsets;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Enterprise Keyset Pagination Extraction Engine.
 * Extracts Extra Large (5M+ rows) datasets from Azure SQL Server in discrete,
 * B-Tree indexed seek chunks and streams them directly into Google Cloud Storage (GCS)
 * with constant O(1) JVM memory utilization.
 */
@Service
public class KeysetExtractionService {

    private static final Logger log = LoggerFactory.getLogger(KeysetExtractionService.class);
    private static final DateTimeFormatter DATE_PARTITION_FORMATTER = DateTimeFormatter.ofPattern("yyyy-MM-dd");

    private final PipelineControlRepository pipelineControlRepository;
    private final NamedParameterJdbcTemplate azureSqlJdbcTemplate;
    private final GcsStreamingService gcsStreamingService;
    private final ObjectMapper objectMapper;

    @Value("${gcp.storage.bucket-name}")
    private String bucketName;

    @Value("${gcp.storage.base-path:retail}")
    private String basePath;

    public KeysetExtractionService(
            PipelineControlRepository pipelineControlRepository,
            @Qualifier("azureSqlNamedJdbcTemplate") NamedParameterJdbcTemplate azureSqlJdbcTemplate,
            GcsStreamingService gcsStreamingService,
            ObjectMapper objectMapper) {
        this.pipelineControlRepository = pipelineControlRepository;
        this.azureSqlJdbcTemplate = azureSqlJdbcTemplate;
        this.gcsStreamingService = gcsStreamingService;
        this.objectMapper = objectMapper;
    }

    /**
     * Executes extraction across all active tables registered in the PostgreSQL pipeline_control store.
     */
    public void extractAllActiveTables() {
        log.info("Starting pipeline extraction across all active tables in PostgreSQL control plane...");
        List<PipelineControl> activePipelines = pipelineControlRepository.findByIsActiveTrue();

        if (activePipelines.isEmpty()) {
            log.warn("No active tables found in pipeline_control. Exiting extraction run.");
            return;
        }

        log.info("Discovered {} active extraction tasks: {}", activePipelines.size(),
                activePipelines.stream().map(PipelineControl::getTableName).toList());

        for (PipelineControl control : activePipelines) {
            try {
                extractTable(control.getTableName());
            } catch (Exception e) {
                log.error("Unhandled error during extraction of table '{}': {}", control.getTableName(), e.getMessage(), e);
                // Continue to next table to ensure independent failure isolation
            }
        }
    }

    /**
     * Executes keyset pagination loop for a specific table until high watermark is caught up.
     *
     * @param tableName Name of source Azure SQL table (e.g. "orders", "order_items")
     */
    public void extractTable(String tableName) {
        log.info("=================================================================");
        log.info("Beginning Keyset Extraction Job: Table = '{}'", tableName);
        log.info("=================================================================");

        // 1. Fetch initial configuration from PostgreSQL
        PipelineControl control = pipelineControlRepository.findById(tableName)
                .orElseThrow(() -> new IllegalArgumentException("Table '" + tableName + "' not found in pipeline_control."));

        if (!Boolean.TRUE.equals(control.getIsActive())) {
            log.info("Table '{}' is marked inactive in pipeline_control. Skipping.", tableName);
            return;
        }

        String watermarkCol = control.getWatermarkColumn();
        int chunkSize = control.getChunkSize();
        long currentWatermark = control.getLastWatermarkValue();
        String datePartition = LocalDate.now().format(DATE_PARTITION_FORMATTER);

        long overallRowsExtracted = 0;
        int chunkSequence = 1;
        long overallStartTime = System.currentTimeMillis();

        while (true) {
            log.info("-----------------------------------------------------------------");
            log.info("Processing Chunk #{}: Table='{}' | Keyset Watermark > {} | ChunkSize={}",
                    chunkSequence, tableName, currentWatermark, chunkSize);

            // Construct Keyset Pagination Query (Seek Method)
            // Uses indexed primary key seek: O(1) B-Tree seek time regardless of table depth
            String querySql = String.format(
                    "SELECT TOP (:chunkSize) * FROM [%s] WITH (NOLOCK) " +
                    "WHERE [%s] > :watermark " +
                    "ORDER BY [%s] ASC",
                    tableName, watermarkCol, watermarkCol
            );

            MapSqlParameterSource params = new MapSqlParameterSource()
                    .addValue("chunkSize", chunkSize)
                    .addValue("watermark", currentWatermark);

            // Formulate GCS Object Destination Path
            String gcsObjectKey = String.format("%s/%s/dt=%s/chunk_watermark_%010d.jsonl",
                    basePath, tableName, datePartition, currentWatermark);

            AtomicInteger chunkRowCount = new AtomicInteger(0);
            AtomicLong newMaxWatermark = new AtomicLong(currentWatermark);
            long chunkStartTime = System.currentTimeMillis();

            // Stream rows directly from Azure SQL into GCS WriteChannel (Zero In-Memory Buffering)
            try (OutputStream gcsOutputStream = gcsStreamingService.openGcsOutputStream(bucketName, gcsObjectKey)) {

                azureSqlJdbcTemplate.query(querySql, params, (ResultSet rs) -> {
                    try {
                        ResultSetMetaData meta = rs.getMetaData();
                        int colCount = meta.getColumnCount();

                        Map<String, Object> rowMap = new HashMap<>(colCount);
                        for (int i = 1; i <= colCount; i++) {
                            String colName = meta.getColumnName(i).toLowerCase();
                            Object colVal = rs.getObject(i);
                            rowMap.put(colName, colVal);
                        }

                        // Update in-flight high watermark
                        Object watermarkVal = rs.getObject(watermarkCol);
                        if (watermarkVal instanceof Number num) {
                            newMaxWatermark.set(num.longValue());
                        }

                        // Write serialized NDJSON line to GCS channel
                        byte[] jsonBytes = objectMapper.writeValueAsBytes(rowMap);
                        gcsOutputStream.write(jsonBytes);
                        gcsOutputStream.write('\n');

                        chunkRowCount.incrementAndGet();

                    } catch (Exception e) {
                        throw new RuntimeException("Serialization failure during GCS streaming: " + e.getMessage(), e);
                    }
                });

                gcsOutputStream.flush();

            } catch (Exception e) {
                log.error("Fatal I/O error writing chunk #{} for table '{}' to GCS: {}",
                        chunkSequence, tableName, e.getMessage(), e);
                throw new RuntimeException("Extraction aborted due to GCS streaming failure.", e);
            }

            int rowsInChunk = chunkRowCount.get();
            long chunkDurationMs = Math.max(1, System.currentTimeMillis() - chunkStartTime);
            double rate = (rowsInChunk / (chunkDurationMs / 1000.0));

            log.info("Chunk #{} Committed to GCS: gs://{}/{} [Rows: {}, Time: {}ms, Rate: {} rows/s]",
                    chunkSequence, bucketName, gcsObjectKey, String.format("%,d", rowsInChunk),
                    chunkDurationMs, String.format("%,.0f", rate));

            // If no rows were returned, extraction has reached current high watermark
            if (rowsInChunk == 0) {
                log.info("Table '{}' extraction reached high-watermark. No further rows to extract.", tableName);
                break;
            }

            // 2. Commit advanced watermark transactionally in PostgreSQL Control DB
            updateWatermarkInControlDb(tableName, newMaxWatermark.get());

            currentWatermark = newMaxWatermark.get();
            overallRowsExtracted += rowsInChunk;
            chunkSequence++;

            // If chunk returned fewer than requested rows, table is fully caught up
            if (rowsInChunk < chunkSize) {
                log.info("Final chunk was partial ({} < {}). High-watermark caught up.", rowsInChunk, chunkSize);
                break;
            }
        }

        long totalDurationMs = Math.max(1, System.currentTimeMillis() - overallStartTime);
        log.info("=================================================================");
        log.info("COMPLETED Table Extraction: '{}' | Total Rows: {} | Duration: {}s",
                tableName, String.format("%,d", overallRowsExtracted), (totalDurationMs / 1000.0));
        log.info("=================================================================");
    }

    /**
     * Atomically advances the high-watermark in PostgreSQL after successful GCS upload.
     */
    @Transactional(transactionManager = "postgresTransactionManager")
    public void updateWatermarkInControlDb(String tableName, long newWatermark) {
        pipelineControlRepository.findById(tableName).ifPresent(control -> {
            control.setLastWatermarkValue(newWatermark);
            pipelineControlRepository.save(control);
            log.info("Updated PostgreSQL Control State: Table='{}' -> New Watermark={}", tableName, newWatermark);
        });
    }
}
