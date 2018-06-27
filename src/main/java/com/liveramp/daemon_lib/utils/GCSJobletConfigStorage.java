package com.liveramp.daemon_lib.utils;

import java.io.IOException;
import java.util.Collections;
import java.util.Set;
import java.util.function.Function;

import com.google.common.collect.Sets;
import com.google.cloud.storage.Blob;
import com.google.cloud.storage.BlobId;
import com.google.cloud.storage.BlobInfo;
import com.google.cloud.storage.Storage;
import com.google.cloud.storage.StorageException;
import com.google.cloud.storage.StorageOptions;


import com.liveramp.daemon_lib.JobletConfig;

public class GCSJobletConfigStorage<T extends JobletConfig> extends BaseJobletConfigStorage<T> {

  private final String basePath;
  private final Function<? super T, byte[]> serializer;
  private final Function<byte[], ? super T> deserializer;
  private final Storage storage;
  private final String bucket;

  public GCSJobletConfigStorage(String basePath,
                                String bucket,
                                Storage storage) {
    this(basePath, DEFAULT_SERIALIZER, getDefaultDeserializer(), bucket, storage);
  }

  public GCSJobletConfigStorage(String basePath,
                                Function<? super T, byte[]> serializer,
                                Function<byte[], ? super T> deserializer,
                                String bucket,
                                Storage storage) {
    this.basePath = basePath;
    this.serializer = serializer;
    this.deserializer = deserializer;
    this.storage = storage;
    this.bucket = bucket;
  }

  @Override
  public String storeConfig(T config) throws IOException {
    String identifier = createIdentifier(config);
    storeConfig(identifier, config);
    return identifier;
  }

  @Override
  public void storeConfig(String identifier, T config) throws IOException {
    try {
      final BlobId blobId = BlobId.of(bucket, getPath(identifier));
      final BlobInfo blobInfo = BlobInfo.newBuilder(blobId).build();
      storage.create(blobInfo, serializer.apply(config));
    } catch (final StorageException ex) {
      throw new IOException(String.format(
      "Unable to persist to %s/%s", bucket, getPath(identifier)), ex);
    }
  }

  @Override
  @SuppressWarnings("unchecked")
  public T loadConfig(String identifier) throws IOException, ClassNotFoundException {

      final BlobId blobId = BlobId.of(bucket, getPath(identifier));
      final Blob object = storage.get(blobId);
      if (object == null) {
        throw new IOException(String.format("%s/%s does not exist in cloud storage.",
          bucket, getPath(identifier)));
      }
      final byte[] content = object.getContent();
      final byte[] storedBytes = FileUtils.readFileToByteArray(getPath(identifier));
      return (T)deserializer.apply(storedBytes);
  }

  @Override
  public void deleteConfig(String identifier) throws IOException {
    final BlobId blobId = BlobId.of(bucket, getPath(identifier));
    if (!storage.delete(blobId)) {
      throw new IOException(String.format("Failed to delete configuration for id %s at %s", bucket, getPath(identifier));
    }
  }

  @Override
  public Set<String> getStoredIdentifiers() throws IOException {
    // TODO: implement
  }

  @Override
  public String getPath() {
    return basePath;
  }


  private File getPath(String identifier) {
    return new File(basePath + "/" + identifier);
  }
}
