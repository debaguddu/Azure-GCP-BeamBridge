package com.beambridge.extraction.config;

import com.google.cloud.storage.Storage;
import com.google.cloud.storage.StorageOptions;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

/**
 * Google Cloud Storage client configuration.
 * Uses Workload Identity on GKE or Application Default Credentials (ADC) in development.
 */
@Configuration
public class GcsConfig {

    @Value("${gcp.project-id:}")
    private String projectId;

    @Bean
    public Storage storage() {
        StorageOptions.Builder builder = StorageOptions.newBuilder();
        if (projectId != null && !projectId.trim().isEmpty()) {
            builder.setProjectId(projectId.trim());
        }
        return builder.build().getService();
    }
}
