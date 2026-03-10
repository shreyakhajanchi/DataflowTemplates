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

    // NOTE: Testing PTransform with Timers and InfiniteOutput in standard
    // TestPipeline is tricky.
    // Standard PassThroughJUnitRunner waits for pipeline to finish, but this
    // transform generates infinite ticks.
    // For unit testing purpose, we are checking if the pipeline constructs without
    // error.
    // Integration tests or specialized TestStream usage would be needed for
    // verifying timing precision.

    DataGeneratorOptions options =
        org.apache.beam.sdk.options.PipelineOptionsFactory.as(DataGeneratorOptions.class);
    options.setQpsPerPartition(100);

    pipeline
        .apply("Trigger", Create.of(new byte[0]))
        .apply("GenerateTicks", new GenerateTicks(options, schemaView));

    // Not running pipeline.run() because it would be an infinite loop or require
    // TestStream with processing time
    // simulation which is complex for Side Input dependency test.
    // This test ensures construction validity.
  }
}
