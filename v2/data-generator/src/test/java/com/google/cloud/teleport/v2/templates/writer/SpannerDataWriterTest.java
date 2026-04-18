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
package com.google.cloud.teleport.v2.templates.writer;

import com.google.cloud.spanner.Mutation;
import com.google.cloud.teleport.v2.templates.model.DataGeneratorColumn;
import com.google.cloud.teleport.v2.templates.model.DataGeneratorTable;
import com.google.cloud.teleport.v2.templates.model.LogicalType;
import java.util.Arrays;
import java.util.List;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.values.Row;
import org.joda.time.Instant;
import org.junit.Assert;
import org.junit.Test;

public class SpannerDataWriterTest {

  @Test
  public void testRowToMutation_dateMapping() {
    SpannerDataWriter writer =
        new SpannerDataWriter(
            "{\"instanceId\":\"inst\",\"databaseId\":\"db\",\"projectId\":\"proj\"}");
    DataGeneratorColumn col =
        DataGeneratorColumn.builder()
            .name("date_col")
            .logicalType(LogicalType.DATE)
            .isNullable(false)
            .isPrimaryKey(false)
            .isGenerated(false)
            .originalType("DATE")
            .build();
    DataGeneratorTable table =
        DataGeneratorTable.builder()
            .name("my_table")
            .columns(com.google.common.collect.ImmutableList.of(col))
            .isRoot(true)
            .primaryKeys(com.google.common.collect.ImmutableList.of())
            .foreignKeys(com.google.common.collect.ImmutableList.of())
            .uniqueKeys(com.google.common.collect.ImmutableList.of())
            .build();

    Schema schema = Schema.builder().addDateTimeField("date_col").build();
    Instant now = Instant.now();
    Row row = Row.withSchema(schema).addValue(now).build();

    Mutation mutation = writer.rowToMutation(table, row);
    Assert.assertEquals("my_table", mutation.getTable());
    // We can't easily inspect mutation values directly without reflection or using a custom
    // matcher,
    // but we can verify it doesn't crash and produces a valid mutation.
    // Spanner Mutation doesn't expose public getters for values easily in a way that is easy to
    // assert.
    // But we can check if it contains the column!
    Assert.assertTrue(mutation.toString().contains("date_col"));
  }

  @Test
  public void testRowToMutation_arrayMapping() {
    SpannerDataWriter writer =
        new SpannerDataWriter(
            "{\"instanceId\":\"inst\",\"databaseId\":\"db\",\"projectId\":\"proj\"}");
    DataGeneratorColumn col =
        DataGeneratorColumn.builder()
            .name("array_col")
            .logicalType(LogicalType.ARRAY)
            .elementType(LogicalType.STRING)
            .isNullable(false)
            .isPrimaryKey(false)
            .isGenerated(false)
            .originalType("ARRAY<STRING>")
            .build();
    DataGeneratorTable table =
        DataGeneratorTable.builder()
            .name("my_table")
            .columns(com.google.common.collect.ImmutableList.of(col))
            .isRoot(true)
            .primaryKeys(com.google.common.collect.ImmutableList.of())
            .foreignKeys(com.google.common.collect.ImmutableList.of())
            .uniqueKeys(com.google.common.collect.ImmutableList.of())
            .build();

    Schema schema = Schema.builder().addIterableField("array_col", Schema.FieldType.STRING).build();
    List<String> vals = Arrays.asList("a", "b");
    Row row = Row.withSchema(schema).addValue(vals).build();

    Mutation mutation = writer.rowToMutation(table, row);
    Assert.assertEquals("my_table", mutation.getTable());
    Assert.assertTrue(mutation.toString().contains("array_col"));
  }

  @Test
  public void testRowToInsertMutation() {
    SpannerDataWriter writer =
        new SpannerDataWriter(
            "{\"instanceId\":\"inst\",\"databaseId\":\"db\",\"projectId\":\"proj\"}");
    DataGeneratorColumn col =
        DataGeneratorColumn.builder()
            .name("id")
            .logicalType(LogicalType.INT64)
            .isNullable(false)
            .isPrimaryKey(true)
            .isGenerated(false)
            .originalType("INT64")
            .build();
    DataGeneratorTable table =
        DataGeneratorTable.builder()
            .name("my_table")
            .columns(com.google.common.collect.ImmutableList.of(col))
            .isRoot(true)
            .primaryKeys(com.google.common.collect.ImmutableList.of("id"))
            .foreignKeys(com.google.common.collect.ImmutableList.of())
            .uniqueKeys(com.google.common.collect.ImmutableList.of())
            .build();

    Schema schema = Schema.builder().addInt64Field("id").build();
    Row row = Row.withSchema(schema).addValue(1L).build();

    Mutation mutation = writer.rowToInsertMutation(table, row);
    Assert.assertEquals("my_table", mutation.getTable());
    Assert.assertTrue(mutation.toString().contains("insert"));
  }

  @Test
  public void testRowToUpdateMutation() {
    SpannerDataWriter writer =
        new SpannerDataWriter(
            "{\"instanceId\":\"inst\",\"databaseId\":\"db\",\"projectId\":\"proj\"}");
    DataGeneratorColumn col =
        DataGeneratorColumn.builder()
            .name("id")
            .logicalType(LogicalType.INT64)
            .isNullable(false)
            .isPrimaryKey(true)
            .isGenerated(false)
            .originalType("INT64")
            .build();
    DataGeneratorTable table =
        DataGeneratorTable.builder()
            .name("my_table")
            .columns(com.google.common.collect.ImmutableList.of(col))
            .isRoot(true)
            .primaryKeys(com.google.common.collect.ImmutableList.of("id"))
            .foreignKeys(com.google.common.collect.ImmutableList.of())
            .uniqueKeys(com.google.common.collect.ImmutableList.of())
            .build();

    Schema schema = Schema.builder().addInt64Field("id").build();
    Row row = Row.withSchema(schema).addValue(1L).build();

    Mutation mutation = writer.rowToUpdateMutation(table, row);
    Assert.assertEquals("my_table", mutation.getTable());
    Assert.assertTrue(mutation.toString().contains("update"));
  }

  @Test
  public void testRowToDeleteMutation() {
    SpannerDataWriter writer =
        new SpannerDataWriter(
            "{\"instanceId\":\"inst\",\"databaseId\":\"db\",\"projectId\":\"proj\"}");
    DataGeneratorColumn col =
        DataGeneratorColumn.builder()
            .name("id")
            .logicalType(LogicalType.INT64)
            .isNullable(false)
            .isPrimaryKey(true)
            .isGenerated(false)
            .originalType("INT64")
            .build();
    DataGeneratorTable table =
        DataGeneratorTable.builder()
            .name("my_table")
            .columns(com.google.common.collect.ImmutableList.of(col))
            .isRoot(true)
            .primaryKeys(com.google.common.collect.ImmutableList.of("id"))
            .foreignKeys(com.google.common.collect.ImmutableList.of())
            .uniqueKeys(com.google.common.collect.ImmutableList.of())
            .build();

    Schema schema = Schema.builder().addInt64Field("id").build();
    Row row = Row.withSchema(schema).addValue(1L).build();

    Mutation mutation = writer.rowToDeleteMutation(table, row);
    Assert.assertEquals("my_table", mutation.getTable());
    Assert.assertTrue(mutation.toString().contains("delete"));
  }

  @Test
  public void testRowToDeleteMutation_missingPk() {
    SpannerDataWriter writer =
        new SpannerDataWriter(
            "{\"instanceId\":\"inst\",\"databaseId\":\"db\",\"projectId\":\"proj\"}");
    DataGeneratorColumn col =
        DataGeneratorColumn.builder()
            .name("id")
            .logicalType(LogicalType.INT64)
            .isNullable(false)
            .isPrimaryKey(true)
            .isGenerated(false)
            .originalType("INT64")
            .build();
    DataGeneratorTable table =
        DataGeneratorTable.builder()
            .name("my_table")
            .columns(com.google.common.collect.ImmutableList.of(col))
            .isRoot(true)
            .primaryKeys(com.google.common.collect.ImmutableList.of("id"))
            .foreignKeys(com.google.common.collect.ImmutableList.of())
            .uniqueKeys(com.google.common.collect.ImmutableList.of())
            .build();

    Schema schema = Schema.builder().addNullableField("id", Schema.FieldType.INT64).build();
    Row row = Row.withSchema(schema).addValue(null).build();

    Assert.assertThrows(IllegalStateException.class, () -> writer.rowToDeleteMutation(table, row));
  }
}
