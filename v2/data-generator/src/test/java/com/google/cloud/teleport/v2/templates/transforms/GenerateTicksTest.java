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

import com.google.cloud.teleport.v2.templates.DataGeneratorOptions;
import com.google.cloud.teleport.v2.templates.model.DataGeneratorSchema;
import com.google.cloud.teleport.v2.templates.model.DataGeneratorTable;
import com.google.cloud.teleport.v2.templates.model.SinkDialect;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import java.io.Serializable;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.View;
import org.apache.beam.sdk.values.PCollectionView;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

@RunWith(JUnit4.class)
public class GenerateTicksTest implements Serializable {

  @Rule
  public final transient TestPipeline pipeline =
      TestPipeline.create().enableAbandonedNodeEnforcement(false);

  @Test
  public void testGenerateTicks() {
    DataGeneratorTable rootTable1 =
        DataGeneratorTable.builder()
            .name("Root1")
            .isRoot(true)
            .qps(10)
            .columns(ImmutableList.of())
            .primaryKeys(ImmutableList.of())
            .foreignKeys(ImmutableList.of())
            .uniqueKeys(ImmutableList.of())
            .build();

    DataGeneratorTable rootTable2 =
        DataGeneratorTable.builder()
            .name("Root2")
            .isRoot(true)
            .qps(10)
            .columns(ImmutableList.of())
            .primaryKeys(ImmutableList.of())
            .foreignKeys(ImmutableList.of())
            .uniqueKeys(ImmutableList.of())
            .build();

    DataGeneratorTable childTable =
        DataGeneratorTable.builder()
            .name("Child")
            .isRoot(false)
            .interleavedInTable("Root1")
            .qps(5)
            .columns(ImmutableList.of())
            .primaryKeys(ImmutableList.of())
            .foreignKeys(ImmutableList.of())
            .uniqueKeys(ImmutableList.of())
            .build();

    DataGeneratorSchema schema =
        DataGeneratorSchema.builder()
            .tables(
                ImmutableMap.of(
                    "Root1", rootTable1,
                    "Root2", rootTable2,
                    "Child", childTable))
            .dialect(SinkDialect.GOOGLE_STANDARD_SQL)
            .build();

    PCollectionView<DataGeneratorSchema> schemaView =
        pipeline.apply("CreateSchema", Create.of(schema)).apply("ViewSchema", View.asSingleton());

    // Integration tests or specialized TestStream usage would be needed for
    // verifying timing precision.
    // However, given the refactoring, we can verify the scale multiplier logic with
    // a bounded Create transform.

    DataGeneratorOptions options =
        org.apache.beam.sdk.options.PipelineOptionsFactory.as(DataGeneratorOptions.class);
    // 20 Root QPS vs 10 Base Tick Rate = Multiplier of 2
    options.setBaseTickRate(10);

    org.apache.beam.sdk.values.PCollection<Long> scaledTicks =
        pipeline
            .apply("Trigger", Create.of(100L, 200L, 300L, 400L, 500L)) // 5 incoming ticks
            .apply("GenerateTicks", new GenerateTicks(options, schemaView));

    // Because baseTickRate is 10 and schema has 20 root QPS, multiplier is exactly 2.
    // 5 incoming ticks * 2 = 10 output ticks.
    org.apache.beam.sdk.transforms.Count.globally();
    org.apache.beam.sdk.values.PCollection<Long> count =
        scaledTicks.apply("Count", org.apache.beam.sdk.transforms.Count.globally());

    org.apache.beam.sdk.testing.PAssert.that(count).containsInAnyOrder(10L);

    pipeline.run().waitUntilFinish();
  }
}
