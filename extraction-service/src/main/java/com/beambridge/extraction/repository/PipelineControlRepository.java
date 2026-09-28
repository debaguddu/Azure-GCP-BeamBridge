package com.beambridge.extraction.repository;

import com.beambridge.extraction.model.PipelineControl;
import jakarta.persistence.LockModeType;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Lock;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;
import org.springframework.stereotype.Repository;

import java.util.List;
import java.util.Optional;

/**
 * Spring Data JPA Repository for managing pipeline_control state in PostgreSQL.
 */
@Repository
public interface PipelineControlRepository extends JpaRepository<PipelineControl, String> {

    /**
     * Retrieve all active tables designated for ingestion.
     */
    List<PipelineControl> findByIsActiveTrue();

    /**
     * Fetch a pipeline control record with a pessimistic write lock (SELECT FOR UPDATE)
     * to safely synchronize watermark updates across concurrent GKE pod executions.
     */
    @Lock(LockModeType.PESSIMISTIC_WRITE)
    @Query("SELECT p FROM PipelineControl p WHERE p.tableName = :tableName")
    Optional<PipelineControl> findWithLockByTableName(@Param("tableName") String tableName);
}
