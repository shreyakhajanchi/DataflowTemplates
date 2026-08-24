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

import com.google.cloud.teleport.v2.templates.model.GeneratedRecord;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import org.apache.beam.sdk.coders.CustomCoder;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.values.Row;

/**
 * A native Beam Coder for {@link GeneratedRecord}.
 *
 * <p>Avoids Java Serialization by encoding the table name as a UTF-8 string and delegating the
 * dynamic {@link Row} encoding to {@link DynamicRowCoder}.
 */
public class GeneratedRecordCoder extends CustomCoder<GeneratedRecord> {

  private static final GeneratedRecordCoder INSTANCE = new GeneratedRecordCoder();

  public static GeneratedRecordCoder of() {
    return INSTANCE;
  }

  @Override
  public void encode(GeneratedRecord value, OutputStream outStream) throws IOException {
    StringUtf8Coder.of().encode(value.getTableName(), outStream);
    DynamicRowCoder.of().encode(value.primaryKeyValues(), outStream);
  }

  @Override
  public GeneratedRecord decode(InputStream inStream) throws IOException {
    String tableName = StringUtf8Coder.of().decode(inStream);
    Row primaryKeyValues = DynamicRowCoder.of().decode(inStream);
    return GeneratedRecord.create(tableName, primaryKeyValues);
  }

  @Override
  public void verifyDeterministic() throws NonDeterministicException {
    StringUtf8Coder.of().verifyDeterministic();
    DynamicRowCoder.of().verifyDeterministic();
  }
}
