/*
 * Copyright (C) 2026 Google LLC
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */
package com.google.cloud.teleport.v2.templates.utils;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.Arrays;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import org.apache.beam.sdk.coders.ByteArrayCoder;
import org.apache.beam.sdk.coders.CustomCoder;
import org.apache.beam.sdk.coders.RowCoder;
import org.apache.beam.sdk.coders.SerializableCoder;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.values.Row;

/**
 * A highly optimized Coder for dynamic {@link Row}s.
 *
 * <p>Unlike {@link RowCoder} which requires the {@link Schema} at pipeline construction time, this
 * coder dynamically encodes the Schema alongside the Row data, allowing it to handle {@link
 * org.apache.beam.sdk.values.PCollection}s containing Rows with mixed or dynamically generated
 * schemas.
 *
 * <p>This implementation caches the serialized schema bytes and RowCoder instances to eliminate the
 * heavy CPU overhead of Java Serialization on a per-element basis.
 */
public class DynamicRowCoder extends CustomCoder<Row> {

  private static final DynamicRowCoder INSTANCE = new DynamicRowCoder();

  // Caches to avoid per-element serialization and object instantiation overhead
  private static final Map<Schema, byte[]> schemaToBytes = new ConcurrentHashMap<>();
  private static final Map<BytesKey, Schema> bytesToSchema = new ConcurrentHashMap<>();
  private static final Map<Schema, RowCoder> coderCache = new ConcurrentHashMap<>();

  public static DynamicRowCoder of() {
    return INSTANCE;
  }

  private byte[] getSerializedSchema(Schema schema) {
    return schemaToBytes.computeIfAbsent(
        schema,
        s -> {
          try {
            ByteArrayOutputStream baos = new ByteArrayOutputStream();
            SerializableCoder.of(Schema.class).encode(s, baos);
            return baos.toByteArray();
          } catch (IOException e) {
            throw new RuntimeException("Failed to serialize Schema", e);
          }
        });
  }

  @Override
  public void encode(Row value, OutputStream outStream) throws IOException {
    Schema schema = value.getSchema();
    byte[] schemaBytes = getSerializedSchema(schema);

    // Encode the schema metadata
    ByteArrayCoder.of().encode(schemaBytes, outStream);

    // Encode the actual row values
    getCoder(schema).encode(value, outStream);
  }

  @Override
  public Row decode(InputStream inStream) throws IOException {
    // Decode the schema metadata
    byte[] schemaBytes = ByteArrayCoder.of().decode(inStream);

    // Recover the schema from cache or deserialize it if seen for the first time
    Schema schema =
        bytesToSchema.computeIfAbsent(
            new BytesKey(schemaBytes),
            k -> {
              try {
                ByteArrayInputStream bais = new ByteArrayInputStream(k.bytes);
                return SerializableCoder.of(Schema.class).decode(bais);
              } catch (IOException e) {
                throw new RuntimeException("Failed to deserialize Schema", e);
              }
            });

    // Decode the row values
    return getCoder(schema).decode(inStream);
  }

  private RowCoder getCoder(Schema schema) {
    return coderCache.computeIfAbsent(schema, RowCoder::of);
  }

  @Override
  public void verifyDeterministic() throws NonDeterministicException {
    throw new NonDeterministicException(
        this,
        "DynamicRowCoder dynamically serializes schemas which rely on Java Serialization and are not deterministic.");
  }

  /** Wrapper for byte[] to be used as a Map key. */
  private static class BytesKey {
    private final byte[] bytes;
    private final int hashCode;

    BytesKey(byte[] bytes) {
      this.bytes = bytes;
      this.hashCode = Arrays.hashCode(bytes);
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) {
        return true;
      }
      if (o == null || getClass() != o.getClass()) {
        return false;
      }
      BytesKey bytesKey = (BytesKey) o;
      return Arrays.equals(bytes, bytesKey.bytes);
    }

    @Override
    public int hashCode() {
      return hashCode;
    }
  }
}
