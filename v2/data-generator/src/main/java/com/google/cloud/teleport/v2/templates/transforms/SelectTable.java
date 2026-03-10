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

import com.google.cloud.teleport.v2.templates.model.DataGeneratorSchema;
import com.google.cloud.teleport.v2.templates.model.DataGeneratorTable;
import java.util.Map;
import java.util.NavigableMap;
import java.util.TreeMap;
import java.util.concurrent.ThreadLocalRandom;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionView;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A {@link PTransform} that selects a table for each tick based on weighted probability. The weight
 * for a table is (table QPS + QPS of its children). The denominator is the total QPS across all
 * tables.
 */
public class SelectTable extends PTransform<PCollection<Long>, PCollection<DataGeneratorTable>> {

  private static final Logger LOG = LoggerFactory.getLogger(SelectTable.class);
  private final PCollectionView<DataGeneratorSchema> schemaView;

  public SelectTable(PCollectionView<DataGeneratorSchema> schemaView) {
    this.schemaView = schemaView;
  }

  @Override
  public PCollection<DataGeneratorTable> expand(PCollection<Long> input) {
    return input.apply(
        "SelectTableFn", ParDo.of(new SelectTableFn(schemaView)).withSideInputs(schemaView));
  }

  static class SelectTableFn extends DoFn<Long, DataGeneratorTable> {
    private static final Logger LOG = LoggerFactory.getLogger(SelectTableFn.class);
    private final PCollectionView<DataGeneratorSchema> schemaView;

    // Cache for table weights: Key = Cumulative Probability (0.0 to 1.0), Value =
    // Table Name
    private transient NavigableMap<Double, DataGeneratorTable> tableSelectionMap;
    // Keep track of which schema version we cached (checking object identity or
    // hash)
    private transient DataGeneratorSchema cachedSchema;

    public SelectTableFn(PCollectionView<DataGeneratorSchema> schemaView) {
      this.schemaView = schemaView;
    }

    @ProcessElement
    public void processElement(ProcessContext c) {
      DataGeneratorSchema schema = c.sideInput(schemaView);

      // Lazily initialize or update cache if schema changes
      // Note: DataGeneratorSchema is likely singleton, but object identity check is
      // fast.
      if (tableSelectionMap == null || cachedSchema != schema) {
        initializeSelectionMap(schema);
        cachedSchema = schema;
      }

      if (tableSelectionMap.isEmpty()) {
        LOG.warn("No tables available for selection.");
        return;
      }

      double randomValue = ThreadLocalRandom.current().nextDouble();
      Map.Entry<Double, DataGeneratorTable> entry = tableSelectionMap.higherEntry(randomValue);

      // Fallback for rounding errors (should not happen if normalized correctly, but
      // safety first)
      if (entry == null) {
        entry = tableSelectionMap.lastEntry();
      }

      c.output(entry.getValue());
    }

    private void initializeSelectionMap(DataGeneratorSchema schema) {
      tableSelectionMap = new TreeMap<>();

      double totalRootQps = 0;

      // 1. Calculate Total QPS for Root tables
      for (DataGeneratorTable table : schema.tables().values()) {
        if (table.isRoot()) {
          totalRootQps += table.qps();
        }
      }

      // 2. Build Selection Map based on Root tables
      for (DataGeneratorTable table : schema.tables().values()) {
        if (table.isRoot()) {
          double weight = table.qps();
          if (totalRootQps > 0) {
            double probability = weight / totalRootQps;
            double previousMax = tableSelectionMap.isEmpty() ? 0.0 : tableSelectionMap.lastKey();
            tableSelectionMap.put(previousMax + probability, table);
          }
        }
      }

      LOG.info(
          "Initialized Table Selection Map with {} entries. Total Root QPS: {}",
          tableSelectionMap.size(),
          totalRootQps);
    }
  }
}
