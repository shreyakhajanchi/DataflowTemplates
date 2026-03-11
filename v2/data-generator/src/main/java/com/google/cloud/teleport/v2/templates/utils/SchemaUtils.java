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
    Map<String, List<String>> parentToSequenceChild = new HashMap<>();
    Set<String> hasSequenceParent = new HashSet<>();

    // 1. Build Dependency Chains for Each Table
    for (DataGeneratorTable childTable : tableMap.values()) {
      String childName = childTable.name();

      // Interleaved Parent takes precedence
      if (childTable.interleavedInTable() != null) {
        String parentName = childTable.interleavedInTable();
        if (tableMap.containsKey(parentName)) {
          parentToSequenceChild.computeIfAbsent(parentName, k -> new ArrayList<>()).add(childName);
          hasSequenceParent.add(childName);
        }
        continue; // Interleaving defines the sole sequence parent
      }

      // Collect Foreign Key Parents
      List<DataGeneratorTable> fkParents = new ArrayList<>();
      for (DataGeneratorForeignKey fk : childTable.foreignKeys()) {
        DataGeneratorTable parentTable = tableMap.get(fk.referencedTable());
        if (parentTable != null) {
          fkParents.add(parentTable);
        }
      }

      if (fkParents.isEmpty()) {
        continue; // No parents for this table
      }

      // Sort FK Parents by QPS
      fkParents.sort(java.util.Comparator.comparingInt(DataGeneratorTable::qps));

      // Chain the FK Parents: P1 -> P2 -> ... -> Pn -> Child
      for (int i = 0; i < fkParents.size() - 1; i++) {
        String currentParentName = fkParents.get(i).name();
        String nextParentName = fkParents.get(i + 1).name();
        // Avoid adding duplicate dependencies if a table is part of multiple chains
        List<String> currentChildren =
            parentToSequenceChild.computeIfAbsent(currentParentName, k -> new ArrayList<>());
        if (!currentChildren.contains(nextParentName)) {
          currentChildren.add(nextParentName);
        }
        hasSequenceParent.add(nextParentName);
      }

      // Link the last parent in the chain to the child table
      String lastParentName = fkParents.get(fkParents.size() - 1).name();
      List<String> lastParentChildren =
          parentToSequenceChild.computeIfAbsent(lastParentName, k -> new ArrayList<>());
      if (!lastParentChildren.contains(childName)) {
        lastParentChildren.add(childName);
      }
      hasSequenceParent.add(childName);
    }

    // 2. Update Tables with Sequence Children and isRoot
    ImmutableMap.Builder<String, DataGeneratorTable> newTablesBuilder = ImmutableMap.builder();
    for (DataGeneratorTable table : tableMap.values()) {
      String tableName = table.name();
      List<String> sequenceChildren =
          parentToSequenceChild.getOrDefault(tableName, ImmutableList.of());
      boolean isRoot = !hasSequenceParent.contains(tableName);

      newTablesBuilder.put(
          tableName,
          table.toBuilder()
              .children(
                  ImmutableList.copyOf(
                      sequenceChildren)) // These are tables to generate AFTER this one
              .isRoot(isRoot)
              .build());
    }

    return DataGeneratorSchema.builder()
        .dialect(schema.dialect())
        .tables(newTablesBuilder.buildOrThrow())
        .build();
  }
}
