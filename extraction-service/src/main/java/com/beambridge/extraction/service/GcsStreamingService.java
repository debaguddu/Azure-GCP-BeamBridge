package com.beambridge.extraction.service;

import com.google.cloud.WriteChannel;
import com.google.cloud.storage.BlobId;
import com.google.cloud.storage.BlobInfo;
import com.google.cloud.storage.Storage;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

import java.io.OutputStream;
import java.nio.channels.Channels;

/**
 * High-performance streaming client for Google Cloud Storage.
 * Writes records directly to GCS WriteChannel without allocating intermediate in-memory byte arrays.
 */
@Service
public class GcsStreamingService {

    private static final Logger log = LoggerFactory.getLogger(GcsStreamingService.class);
    private final Storage storage;

    public GcsStreamingService(Storage storage) {
        this.storage = storage;
    }

    /**
     * Opens an active streaming OutputStream to a designated GCS object.
     *
     * @param bucketName Target GCS bucket
     * @param objectPath Full object key within the bucket (e.g. retail/orders/dt=2026-09-28/chunk_001.jsonl)
     * @return OutputStream wrapping the underlying GCS WriteChannel
     */
    public OutputStream openGcsOutputStream(String bucketName, String objectPath) {
        log.info("Opening GCS streaming channel to: gs://{}/{}", bucketName, objectPath);

        BlobId blobId = BlobId.of(bucketName, objectPath);
        BlobInfo blobInfo = BlobInfo.newBuilder(blobId)
                .setContentType("application/x-ndjson")
                .setContentEncoding("UTF-8")
                .build();

        WriteChannel writeChannel = storage.writer(blobInfo);
        return Channels.newOutputStream(writeChannel);
    }

    /**
     * Checks if a designated object already exists in GCS.
     */
    public boolean objectExists(String bucketName, String objectPath) {
        BlobId blobId = BlobId.of(bucketName, objectPath);
        return storage.get(blobId) != null;
    }
}
