package com.liveramp.daemon_lib.utils;

import java.io.IOException;
import java.util.Collections;
import java.util.Set;
import java.util.function.Function;

import com.google.common.collect.Sets;
import com.google.cloud.storage.Blob;
import com.google.cloud.storage.BlobId;
import com.google.cloud.storage.BlobInfo;
import com.google.cloud.storage.Bucket;
import com.google.cloud.storage.Storage;
import com.google.cloud.storage.StorageException;
import com.google.cloud.storage.StorageOptions;


import com.liveramp.daemon_lib.JobletConfig;

public class GCSJobletConfigStorage<T extends JobletConfig> extends BaseJobletConfigStorage<T> {

  private final String basePath;
  private final Function<? super T, byte[]> serializer;
  private final Function<byte[], ? super T> deserializer;
  private final Bucket bucket;

  public GCSJobletConfigStorage(String basePath, String bucket) {
    this(basePath, DEFAULT_SERIALIZER, getDefaultDeserializer(), bucket);
  }

  public GCSJobletConfigStorage(String basePath,
                                Function<? super T, byte[]> serializer,
                                Function<byte[], ? super T> deserializer,
                                String bucket) {
    this.basePath = basePath;
    this.serializer = serializer;
    this.deserializer = deserializer;
    this.bucket = Bucket.newBuilder(bucket).build();
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
      final BlobId blobId = BlobId.of(this.bucket.getName(), getPath(identifier));
      final BlobInfo blobInfo = BlobInfo.newBuilder(blobId).build();
      this.bucket.getStorage().create(blobInfo, serializer.apply(config));
    } catch (final StorageException ex) {
      throw new IOException(String.format(
      "Unable to persist to %s/%s", this.bucket.getName(), getPath(identifier)), ex);
    }
  }

  @Override
  @SuppressWarnings("unchecked")
  public T loadConfig(String identifier) throws IOException, ClassNotFoundException {
      final BlobId blobId = BlobId.of(this.bucket.getName(), getPath(identifier));
      final Blob blob = this.bucket.getStorage().get(blobId);
      if (object == null) {
        throw new IOException(String.format("%s/%s does not exist in cloud storage.",
          this.bucket.getName(), getPath(identifier)));
      }
      return (T)deserializer.apply(blob.getContent());
  }

  @Override
  public void deleteConfig(String identifier) throws IOException {
    final BlobId blobId = BlobId.of(this.bucket.getName(), getPath(identifier));
    if (!storage.delete(blobId)) {
      throw new IOException(String.format("Failed to delete configuration for id %s at %s",
                            this.bucket.getName(), getPath(identifier));
    }
  }

  @Override
  public Set<String> getStoredIdentifiers() throws IOException {
    final Page<Blob> blobs = this.Bucket.List(Storage.BlobListOption.prefix(this.basePath));
    idSet = (String)Sets.newHashSet();
    while (blobs.hasNext()) {
      idSet.add((String)blobs.next());
    }
    return idSet
  }

  @Override
  public String getPath() {
    return basePath;
  }

  private File getPath(String identifier) {
    return new File(basePath + "/" + identifier);
  }
}
