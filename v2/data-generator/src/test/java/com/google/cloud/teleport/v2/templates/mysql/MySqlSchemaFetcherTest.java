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
package com.google.cloud.teleport.v2.templates.mysql;

import static org.junit.Assert.assertEquals;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.google.cloud.teleport.v2.templates.model.DataGeneratorSchema;
import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.Spy;
import org.mockito.junit.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.class)
public class MySqlSchemaFetcherTest {

  @Mock private Connection connection;
  @Mock private ResultSet resultSet;

  @Spy private MySqlSchemaFetcher fetcher;

  @Before
  public void setUp() throws IOException {
    File tmpFile = File.createTempFile("shard", ".json");
    tmpFile.deleteOnExit();
    String shardJson =
        "[{\"logicalShardId\": \"shard1\", \"host\": \"localhost\", \"port\": \"3306\", \"user\": \"root\", \"password\": \"pass\", \"dbNameToLogicalShardIdMap\": {\"db\": \"db1\"}, \"secretManagerUri\": \"\"}]";
    Files.write(tmpFile.toPath(), shardJson.getBytes(StandardCharsets.UTF_8));
    fetcher.init(tmpFile.getAbsolutePath(), shardJson);
  }

  @Test
  public void testGetSchemaMySql() throws IOException, SQLException {
    File tmpFile = File.createTempFile("shard", ".json");
    tmpFile.deleteOnExit();
    String shardJson =
        "[{\"logicalShardId\": \"shard1\", \"host\": \"localhost\", \"port\": \"3306\", \"user\": \"root\", \"password\": \"pass\", \"dbNameToLogicalShardIdMap\": {\"db\": \"db1\"}, \"secretManagerUri\": \"\"}]";
    Files.write(tmpFile.toPath(), shardJson.getBytes(StandardCharsets.UTF_8));
    fetcher.init(tmpFile.getAbsolutePath(), shardJson);

    // Mock connection
    doReturn(connection).when(fetcher).getConnection();
    when(connection.getCatalog()).thenReturn("db");

    // Mock scanner
    com.google.cloud.teleport.v2.spanner.sourceddl.MySqlInformationSchemaScanner mockScanner =
        mock(com.google.cloud.teleport.v2.spanner.sourceddl.MySqlInformationSchemaScanner.class);
    doReturn(mockScanner).when(fetcher).createMySqlScanner(eq(connection), eq("db"));

    // Prepare SourceSchema mock
    com.google.cloud.teleport.v2.spanner.sourceddl.SourceSchema mockSourceSchema =
        mock(com.google.cloud.teleport.v2.spanner.sourceddl.SourceSchema.class);
    when(mockScanner.scan()).thenReturn(mockSourceSchema);

    // Mock Tables
    com.google.cloud.teleport.v2.spanner.sourceddl.SourceTable mockTable =
        mock(com.google.cloud.teleport.v2.spanner.sourceddl.SourceTable.class);
    when(mockTable.name()).thenReturn("t");
    when(mockTable.primaryKeyColumns())
        .thenReturn(com.google.common.collect.ImmutableList.of("id"));

    // Mock Columns
    com.google.cloud.teleport.v2.spanner.sourceddl.SourceColumn mockColumn =
        mock(com.google.cloud.teleport.v2.spanner.sourceddl.SourceColumn.class);
    when(mockColumn.name()).thenReturn("id");
    when(mockColumn.type()).thenReturn("int");
    when(mockColumn.isNullable()).thenReturn(false);
    when(mockColumn.isPrimaryKey()).thenReturn(true);
    when(mockTable.columns()).thenReturn(com.google.common.collect.ImmutableList.of(mockColumn));

    when(mockSourceSchema.tables())
        .thenReturn(com.google.common.collect.ImmutableMap.of("t", mockTable));

    DataGeneratorSchema result = fetcher.getSchema();

    assertEquals(1, result.tables().size());
    assertEquals("t", result.tables().get("t").name());
    assertEquals("id", result.tables().get("t").columns().get(0).name());
    verify(connection).close();
  }

  @Test
  public void testGetSchemaMySqlWithFKAndUK() throws IOException, SQLException {
    File tmpFile = File.createTempFile("shard", ".json");
    tmpFile.deleteOnExit();
    String shardJson =
        "[{\"logicalShardId\": \"shard1\", \"host\": \"localhost\", \"port\": \"3306\", \"user\": \"root\", \"password\": \"pass\", \"dbNameToLogicalShardIdMap\": {\"db\": \"db1\"}, \"secretManagerUri\": \"\"}]";
    Files.write(tmpFile.toPath(), shardJson.getBytes(StandardCharsets.UTF_8));
    fetcher.init(tmpFile.getAbsolutePath(), shardJson);

    // Mock connection
    doReturn(connection).when(fetcher).getConnection();
    when(connection.getCatalog()).thenReturn("db");

    // Mock scanner
    com.google.cloud.teleport.v2.spanner.sourceddl.MySqlInformationSchemaScanner mockScanner =
        mock(com.google.cloud.teleport.v2.spanner.sourceddl.MySqlInformationSchemaScanner.class);
    doReturn(mockScanner).when(fetcher).createMySqlScanner(eq(connection), eq("db"));

    // Prepare SourceSchema mock
    com.google.cloud.teleport.v2.spanner.sourceddl.SourceSchema mockSourceSchema =
        mock(com.google.cloud.teleport.v2.spanner.sourceddl.SourceSchema.class);
    when(mockScanner.scan()).thenReturn(mockSourceSchema);

    // Mock Parent Table
    com.google.cloud.teleport.v2.spanner.sourceddl.SourceTable parentTable =
        mock(com.google.cloud.teleport.v2.spanner.sourceddl.SourceTable.class);
    when(parentTable.name()).thenReturn("Parent");
    when(parentTable.primaryKeyColumns())
        .thenReturn(com.google.common.collect.ImmutableList.of("id"));

    com.google.cloud.teleport.v2.spanner.sourceddl.SourceColumn parentIdCol =
        mock(com.google.cloud.teleport.v2.spanner.sourceddl.SourceColumn.class);
    when(parentIdCol.name()).thenReturn("id");
    when(parentIdCol.type()).thenReturn("int");
    when(parentTable.columns()).thenReturn(com.google.common.collect.ImmutableList.of(parentIdCol));

    // Mock Child Table
    com.google.cloud.teleport.v2.spanner.sourceddl.SourceTable childTable =
        mock(com.google.cloud.teleport.v2.spanner.sourceddl.SourceTable.class);
    when(childTable.name()).thenReturn("Child");
    when(childTable.primaryKeyColumns())
        .thenReturn(com.google.common.collect.ImmutableList.of("id"));

    com.google.cloud.teleport.v2.spanner.sourceddl.SourceColumn childIdCol =
        mock(com.google.cloud.teleport.v2.spanner.sourceddl.SourceColumn.class);
    when(childIdCol.name()).thenReturn("id");
    when(childIdCol.type()).thenReturn("int");

    com.google.cloud.teleport.v2.spanner.sourceddl.SourceColumn childParentIdCol =
        mock(com.google.cloud.teleport.v2.spanner.sourceddl.SourceColumn.class);
    when(childParentIdCol.name()).thenReturn("parentId");
    when(childParentIdCol.type()).thenReturn("int");

    when(childTable.columns())
        .thenReturn(com.google.common.collect.ImmutableList.of(childIdCol, childParentIdCol));

    // Mock FK
    com.google.cloud.teleport.v2.spanner.sourceddl.SourceForeignKey fk =
        com.google.cloud.teleport.v2.spanner.sourceddl.SourceForeignKey.builder()
            .name("fk_parent")
            .referencedTable("Parent")
            .keyColumns(com.google.common.collect.ImmutableList.of("parentId"))
            .referencedColumns(com.google.common.collect.ImmutableList.of("id"))
            .build();
    when(childTable.foreignKeys()).thenReturn(com.google.common.collect.ImmutableList.of(fk));

    // Mock UK
    com.google.cloud.teleport.v2.spanner.sourceddl.SourceUniqueKey uk =
        com.google.cloud.teleport.v2.spanner.sourceddl.SourceUniqueKey.builder()
            .name("uk_unique")
            .keyColumns(com.google.common.collect.ImmutableList.of("id"))
            .build();
    when(childTable.uniqueKeys()).thenReturn(com.google.common.collect.ImmutableList.of(uk));

    when(mockSourceSchema.tables())
        .thenReturn(
            com.google.common.collect.ImmutableMap.of("Parent", parentTable, "Child", childTable));

    DataGeneratorSchema result = fetcher.getSchema();

    assertEquals(2, result.tables().size());
    com.google.cloud.teleport.v2.templates.model.DataGeneratorTable childResult =
        result.tables().get("Child");
    assertEquals(1, childResult.foreignKeys().size());
    assertEquals("fk_parent", childResult.foreignKeys().get(0).name());
    assertEquals(1, childResult.uniqueKeys().size());
    assertEquals("uk_unique", childResult.uniqueKeys().get(0).name());
  }
}
