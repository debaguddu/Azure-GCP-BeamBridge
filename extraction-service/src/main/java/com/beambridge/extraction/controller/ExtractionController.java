package com.beambridge.extraction.controller;

import com.beambridge.extraction.model.PipelineControl;
import com.beambridge.extraction.repository.PipelineControlRepository;
import com.beambridge.extraction.service.KeysetExtractionService;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;

/**
 * REST API for pipeline inspection, ad-hoc execution triggers, and operational management.
 */
@RestController
@RequestMapping("/api/v1/pipeline")
public class ExtractionController {

    private final KeysetExtractionService extractionService;
    private final PipelineControlRepository pipelineControlRepository;

    public ExtractionController(KeysetExtractionService extractionService,
                                PipelineControlRepository pipelineControlRepository) {
        this.extractionService = extractionService;
        this.pipelineControlRepository = pipelineControlRepository;
    }

    /**
     * Inspect current status and watermarks for all registered pipelines.
     */
    @GetMapping("/status")
    public ResponseEntity<List<PipelineControl>> getStatus() {
        return ResponseEntity.ok(pipelineControlRepository.findAll());
    }

    /**
     * Trigger extraction for all active tables asynchronously.
     */
    @PostMapping("/extract")
    public ResponseEntity<Map<String, String>> triggerAll() {
        CompletableFuture.runAsync(extractionService::extractAllActiveTables);
        Map<String, String> response = new HashMap<>();
        response.put("status", "ACCEPTED");
        response.put("message", "Extraction triggered asynchronously for all active tables.");
        return ResponseEntity.accepted().body(response);
    }

    /**
     * Trigger extraction for a specific table.
     */
    @PostMapping("/extract/{tableName}")
    public ResponseEntity<Map<String, String>> triggerTable(@PathVariable String tableName) {
        CompletableFuture.runAsync(() -> extractionService.extractTable(tableName));
        Map<String, String> response = new HashMap<>();
        response.put("status", "ACCEPTED");
        response.put("table", tableName);
        response.put("message", "Extraction initiated asynchronously for table: " + tableName);
        return ResponseEntity.accepted().body(response);
    }
}
