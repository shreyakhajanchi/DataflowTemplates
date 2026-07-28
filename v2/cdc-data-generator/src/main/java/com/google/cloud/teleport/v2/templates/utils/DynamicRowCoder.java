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

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
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
 * <p>This replaces the slow fallback to {@link org.apache.beam.sdk.coders.SerializableCoder} which
 * uses heavy Java Serialization.
 */
public class DynamicRowCoder extends CustomCoder<Row> {

  private static final DynamicRowCoder INSTANCE = new DynamicRowCoder();

  public static DynamicRowCoder of() {
    return INSTANCE;
  }

  @Override
  public void encode(Row value, OutputStream outStream) throws IOException {
    Schema schema = value.getSchema();
    // Encode the schema (Schema implements Serializable)
    SerializableCoder.of(Schema.class).encode(schema, outStream);
    // Encode the actual row values using the dynamically loaded schema
    RowCoder.of(schema).encode(value, outStream);
  }

  @Override
  public Row decode(InputStream inStream) throws IOException {
    // Decode the schema first
    Schema schema = SerializableCoder.of(Schema.class).decode(inStream);
    // Decode the row values using the recovered schema
    return RowCoder.of(schema).decode(inStream);
  }

  @Override
  public void verifyDeterministic() throws NonDeterministicException {
    SerializableCoder.of(Schema.class).verifyDeterministic();
  }
}
