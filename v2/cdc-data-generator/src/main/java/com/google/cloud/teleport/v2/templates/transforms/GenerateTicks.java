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
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionView;
import org.joda.time.Instant;

/**
 * A {@link PTransform} that takes a trigger tick and outputs multiple ticks
 * based on the total QPS required by the root tables in the schema side input.
 */
public class GenerateTicks extends PTransform<PCollection<Instant>, PCollection<Long>> {

  private final PCollectionView<DataGeneratorSchema> schemaView;

  public GenerateTicks(PCollectionView<DataGeneratorSchema> schemaView) {
    this.schemaView = schemaView;
  }

  @Override
  public PCollection<Long> expand(PCollection<Instant> input) {
    return input.apply(
        "ScaleTicks",
        ParDo.of(new ScaleTicksFn(schemaView)).withSideInputs(schemaView));
  }

  static class ScaleTicksFn extends DoFn<Instant, Long> {
    private final PCollectionView<DataGeneratorSchema> schemaView;
    private transient DataGeneratorSchema cachedSchema;
    private transient int cachedTotalQps;

    ScaleTicksFn(PCollectionView<DataGeneratorSchema> schemaView) {
      this.schemaView = schemaView;
    }

    @ProcessElement
    public void processElement(ProcessContext c) {
      DataGeneratorSchema schema = c.sideInput(schemaView);

      // Calculate total QPS only if schema changes (usually it's static)
      if (cachedSchema != schema) {
        int totalQps = 0;
        for (DataGeneratorTable table : schema.tables().values()) {
          if (table.isRoot()) {
            totalQps += table.insertQps();
          }
        }
        cachedTotalQps = totalQps;
        cachedSchema = schema;
      }

      if (cachedTotalQps <= 0) {
        return; // No generation
      }

      for (int i = 0; i < cachedTotalQps; i++) {
        c.output(c.element().getMillis());
      }
    }
  }
}
