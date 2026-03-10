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

import com.google.cloud.teleport.v2.templates.model.DataGeneratorForeignKey;
import com.google.cloud.teleport.v2.templates.model.DataGeneratorSchema;
import com.google.cloud.teleport.v2.templates.model.DataGeneratorTable;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/** Utilities for manipulating {@link DataGeneratorSchema}. */
public class SchemaUtils {

  /**
   * Constructs a Directed Acyclic Graph (DAG) of tables in the schema. Identifies parent-child
   * relationships based on Foreign Keys and Interleaving. Handles multiple parents by selecting the
   * one with the *least* QPS. Populates the `children` list for each table and sets `isRoot`
   * accordingly.
   *
   * @param schema The input schema.
   * @return A new schema with DAG information populated.
   */
  public static DataGeneratorSchema setSchemaDAG(DataGeneratorSchema schema) {
    Map<String, DataGeneratorTable> tableMap = schema.tables();
    Map<String, List<String>> parentToChildren =
        new HashMap<>(); // Parent Name -> List of Child Names
    Set<String> allChildren = new HashSet<>();

    // 1. Identify Relationships
    for (DataGeneratorTable table : tableMap.values()) {
      String tableName = table.name();
      String bestParent = null;
      int minParentQps = Integer.MAX_VALUE;

      // Check Interleaving (Strict Parent)
      if (table.interleavedInTable() != null) {
        bestParent = table.interleavedInTable();
      } else {
        // Check Foreign Keys
        for (DataGeneratorForeignKey fk : table.foreignKeys()) {
          String parentName = fk.referencedTable();
          DataGeneratorTable parentTable = tableMap.get(parentName);
          if (parentTable != null) {
            // If multiple parents, pick the one with lower QPS to avoid bottlenecking
            // or effectively, the "main" driver.
            if (parentTable.qps() < minParentQps) {
              minParentQps = parentTable.qps();
              bestParent = parentName;
            }
          }
        }
      }

      if (bestParent != null) {
        parentToChildren.computeIfAbsent(bestParent, k -> new ArrayList<>()).add(tableName);
        allChildren.add(tableName);
      }
    }

    // 2. Update Tables with Children Names and isRoot
    ImmutableMap.Builder<String, DataGeneratorTable> newTablesBuilder = ImmutableMap.builder();
    for (DataGeneratorTable table : tableMap.values()) {
      String tableName = table.name();
      List<String> childrenNames = parentToChildren.getOrDefault(tableName, ImmutableList.of());
      boolean isRoot = !allChildren.contains(tableName);

      newTablesBuilder.put(
          tableName,
          table.toBuilder().children(ImmutableList.copyOf(childrenNames)).isRoot(isRoot).build());
    }

    return DataGeneratorSchema.builder()
        .dialect(schema.dialect())
        .tables(newTablesBuilder.buildOrThrow())
        .build();
  }
}
