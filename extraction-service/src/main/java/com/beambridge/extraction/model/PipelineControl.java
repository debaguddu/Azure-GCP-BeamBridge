package com.beambridge.extraction.model;

import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.Id;
import jakarta.persistence.Table;
import java.time.Instant;

/**
 * Entity mapping to the PostgreSQL control plane table: pipeline_control.
 * Tracks table-level extraction state, high watermarks, and chunking parameters.
 */
@Entity
@Table(name = "pipeline_control")
public class PipelineControl {

    @Id
    @Column(name = "table_name", length = 100, nullable = false)
    private String tableName;

    @Column(name = "is_active", nullable = false)
    private Boolean isActive;

    @Column(name = "extract_type", length = 20)
    private String extractType; // e.g. "INCREMENTAL" or "FULL"

    @Column(name = "watermark_column", length = 50)
    private String watermarkColumn; // e.g. "order_id", "customer_id"

    @Column(name = "last_watermark_value")
    private Long lastWatermarkValue;

    @Column(name = "chunk_size")
    private Integer chunkSize;

    @Column(name = "gcs_landing_path", length = 255)
    private String gcsLandingPath;

    // Constructors
    public PipelineControl() {
    }

    public PipelineControl(String tableName, Boolean isActive, String extractType,
                           String watermarkColumn, Long lastWatermarkValue,
                           Integer chunkSize, String gcsLandingPath) {
        this.tableName = tableName;
        this.isActive = isActive;
        this.extractType = extractType;
        this.watermarkColumn = watermarkColumn;
        this.lastWatermarkValue = lastWatermarkValue;
        this.chunkSize = chunkSize;
        this.gcsLandingPath = gcsLandingPath;
    }

    // Getters and Setters
    public String getTableName() {
        return tableName;
    }

    public void setTableName(String tableName) {
        this.tableName = tableName;
    }

    public Boolean getIsActive() {
        return isActive;
    }

    public void setIsActive(Boolean isActive) {
        this.isActive = isActive;
    }

    public String getExtractType() {
        return extractType;
    }

    public void setExtractType(String extractType) {
        this.extractType = extractType;
    }

    public String getWatermarkColumn() {
        return watermarkColumn;
    }

    public void setWatermarkColumn(String watermarkColumn) {
        this.watermarkColumn = watermarkColumn;
    }

    public Long getLastWatermarkValue() {
        return lastWatermarkValue != null ? lastWatermarkValue : 0L;
    }

    public void setLastWatermarkValue(Long lastWatermarkValue) {
        this.lastWatermarkValue = lastWatermarkValue;
    }

    public Integer getChunkSize() {
        return (chunkSize != null && chunkSize > 0) ? chunkSize : 50000;
    }

    public void setChunkSize(Integer chunkSize) {
        this.chunkSize = chunkSize;
    }

    public String getGcsLandingPath() {
        return gcsLandingPath;
    }

    public void setGcsLandingPath(String gcsLandingPath) {
        this.gcsLandingPath = gcsLandingPath;
    }

    @Override
    public String toString() {
        return "PipelineControl{" +
                "tableName='" + tableName + '\'' +
                ", isActive=" + isActive +
                ", extractType='" + extractType + '\'' +
                ", watermarkColumn='" + watermarkColumn + '\'' +
                ", lastWatermarkValue=" + lastWatermarkValue +
                ", chunkSize=" + chunkSize +
                ", gcsLandingPath='" + gcsLandingPath + '\'' +
                '}';
    }
}
