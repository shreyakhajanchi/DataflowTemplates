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
package com.google.cloud.teleport.v2.templates.transforms;

import com.google.cloud.teleport.v2.templates.model.DataGeneratorColumn;
import com.google.cloud.teleport.v2.templates.model.DataGeneratorTable;
import com.google.cloud.teleport.v2.templates.model.LogicalType;
import com.google.common.collect.ImmutableList;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.Row;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

@RunWith(JUnit4.class)
public class GeneratePrimaryKeyTest {
  @Rule public final transient TestPipeline pipeline = TestPipeline.create();

  @Test
  public void testGeneratePrimaryKey() {
    DataGeneratorColumn pkCol1 =
        DataGeneratorColumn.builder()
            .name("id")
            .logicalType(LogicalType.STRING)
            .isPrimaryKey(true)
            .isNullable(false)
            .isGenerated(false)
            .originalType("VARCHAR")
            .size(10L)
            .build();

    DataGeneratorColumn pkCol2 =
        DataGeneratorColumn.builder()
            .name("seq")
            .logicalType(LogicalType.INT64)
            .isPrimaryKey(true)
            .isNullable(false)
            .isGenerated(false)
            .originalType("BIGINT")
            .build();

    DataGeneratorColumn valCol =
        DataGeneratorColumn.builder()
            .name("val")
            .logicalType(LogicalType.STRING)
            .isPrimaryKey(false)
            .isNullable(true)
            .isGenerated(false)
            .originalType("VARCHAR")
            .build();

    DataGeneratorTable table =
        DataGeneratorTable.builder()
            .name("TestTable")
            .columns(ImmutableList.of(pkCol1, pkCol2, valCol))
            .qps(10)
            .isRoot(true)
            .primaryKeys(ImmutableList.of("id", "seq"))
            .foreignKeys(ImmutableList.of())
            .uniqueKeys(ImmutableList.of())
            .build();

    PCollection<DataGeneratorTable> input = pipeline.apply(Create.of(table));

    PCollection<KV<String, Row>> output = input.apply(new GeneratePrimaryKey());

    // Define the expected schema for the output rows
    org.apache.beam.sdk.schemas.Schema rowSchema =
        org.apache.beam.sdk.schemas.Schema.builder()
            .addField(
                org.apache.beam.sdk.schemas.Schema.Field.of(
                    "id", org.apache.beam.sdk.schemas.Schema.FieldType.STRING))
            .addField(
                org.apache.beam.sdk.schemas.Schema.Field.of(
                    "seq", org.apache.beam.sdk.schemas.Schema.FieldType.INT64))
            .build();

    // Set the coder for the output PCollection
    output.setCoder(
        org.apache.beam.sdk.coders.KvCoder.of(
            org.apache.beam.sdk.coders.StringUtf8Coder.of(),
            org.apache.beam.sdk.coders.RowCoder.of(rowSchema)));

    PAssert.that(output)
        .satisfies(
            collection -> {
              if (!collection.iterator().hasNext()) {
                throw new AssertionError("No output generated");
              }
              KV<String, Row> element = collection.iterator().next();
              Row row = element.getValue();

              // Verify key (table name)
              if (!element.getKey().equals("TestTable")) {
                throw new AssertionError("Key should be TestTable, but was " + element.getKey());
              }

              // Verify schema
              if (row.getSchema().getFieldCount() != 2) {
                throw new AssertionError(
                    "Schema should have 2 fields (PKs only), but had "
                        + row.getSchema().getFieldCount());
              }
              if (!row.getSchema().getField(0).getName().equals("id")) {
                throw new AssertionError("First field should be id");
              }
              if (!row.getSchema().getField(1).getName().equals("seq")) {
                throw new AssertionError("Second field should be seq");
              }

              // Verify values
              String id = row.getString("id");
              Long seq = row.getInt64("seq");

              if (id == null || id.isEmpty()) {
                throw new AssertionError("ID should not be empty");
              }
              if (seq == null) {
                throw new AssertionError("Seq should not be null");
              }

              return null;
            });

    pipeline.run();
  }
}
