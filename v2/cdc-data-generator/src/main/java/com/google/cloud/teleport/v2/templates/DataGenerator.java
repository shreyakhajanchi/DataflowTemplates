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
package com.google.cloud.teleport.v2.templates;

import com.google.cloud.teleport.metadata.Template;
import com.google.cloud.teleport.metadata.TemplateCategory;
import com.google.cloud.teleport.v2.common.UncaughtExceptionLogger;
import com.google.cloud.teleport.v2.templates.model.DataGeneratorSchema;
import com.google.cloud.teleport.v2.templates.model.DataGeneratorTable;
import com.google.cloud.teleport.v2.templates.model.GeneratedRecord;
import com.google.cloud.teleport.v2.templates.transforms.BatchAndWrite;
import com.google.cloud.teleport.v2.templates.transforms.GeneratePrimaryKey;
import com.google.cloud.teleport.v2.templates.transforms.GenerateTicks;
import com.google.cloud.teleport.v2.templates.transforms.SchemaLoader;
import com.google.cloud.teleport.v2.templates.transforms.SelectTable;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.PipelineResult;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.Redistribute;
import org.apache.beam.sdk.transforms.Values;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionView;
import org.apache.beam.sdk.values.Row;

@Template(
    name = "Data_Generator",
    category = TemplateCategory.STREAMING,
    displayName = "Data Generator",
    description = "A template to generate synthetic data based on a source schema.",
    optionsClass = DataGeneratorOptions.class,
    flexContainerName = "data-generator",
    contactInformation = "https://cloud.google.com/support",
    streaming = true,
    supportsAtLeastOnce = true)
public class DataGenerator {

  public static void main(String[] args) {
    UncaughtExceptionLogger.register();

    DataGeneratorOptions options =
        PipelineOptionsFactory.fromArgs(args).withValidation().as(DataGeneratorOptions.class);
    options.setStreaming(true);
    run(options);
  }

  public static PipelineResult run(DataGeneratorOptions options) {
    Pipeline pipeline = Pipeline.create(options);

    // Register SerializableCoder for Row class globally since we use dynamically
    // constructed
    // schemas at runtime
    pipeline
        .getCoderRegistry()
        .registerCoderForClass(
            Row.class, org.apache.beam.sdk.coders.SerializableCoder.of(Row.class));

    PCollectionView<DataGeneratorSchema> schemaView =
        pipeline.apply(
            "LoadSchema",
            new SchemaLoader(
                options.getSinkType(),
                options.getSinkOptions(),
                options.getInsertQps(),
                options.getUpdateQps(),
                options.getDeleteQps(),
                options.getSchemaConfig()));

    int keyParallelism = options.getKeyParallelism() != null ? options.getKeyParallelism() : 500;

    // Generate ticks based on schema QPS
    PCollection<DataGeneratorTable> ticks =
        pipeline
            .apply(
                "TriggerTick",
                org.apache.beam.sdk.transforms.PeriodicImpulse.create()
                    .withInterval(org.joda.time.Duration.standardSeconds(1)))
            .apply("GenerateTicks", new GenerateTicks(schemaView))
            .apply(
                "MapToRandomKey",
                ParDo.of(
                    new DoFn<Long, KV<Integer, Long>>() {
                      @ProcessElement
                      public void processElement(ProcessContext c) {
                        int key =
                            java.util.concurrent.ThreadLocalRandom.current()
                                .nextInt(keyParallelism);
                        c.output(KV.of(key, c.element()));
                      }
                    }))
            .apply("RedistributeProvider", Redistribute.byKey())
            .apply("ExtractTickValues", Values.create())
            .apply("SelectTable", new SelectTable(schemaView));

    // Generate Primary Keys
    int maxShards = options.getMaxShards() != null ? options.getMaxShards() : 1;
    PCollection<KV<String, Row>> pendingRows =
        ticks.apply(
            "GeneratePrimaryKey",
            new GeneratePrimaryKey(
                maxShards, options.getSinkOptions(), options.getSinkType().name()));

    // Reshuffle based on Hash(TableName + PK) to ensure same PK goes to same worker
    PCollection<KV<Integer, GeneratedRecord>> reshuffledRows =
        pendingRows
            .apply(
                "MapToReshuffleKey",
                ParDo.of(
                    new DoFn<KV<String, Row>, KV<Integer, GeneratedRecord>>() {
                      @ProcessElement
                      public void processElement(ProcessContext c) {
                        String tableName = c.element().getKey();
                        Row pkValues = c.element().getValue();
                        int hash = (tableName + pkValues.toString()).hashCode();
                        int shard = Math.abs(hash % keyParallelism);
                        c.output(KV.of(shard, GeneratedRecord.create(tableName, pkValues)));
                      }
                    }))
            .apply("Redistribute", Redistribute.byKey());

    Integer updateIntervalMs =
        (options.getUpdateInterval() != null && options.getUpdateInterval() >= 0
                ? options.getUpdateInterval()
                : 5)
            * 1000;
    Integer deleteIntervalMs =
        (options.getDeleteInterval() != null && options.getDeleteInterval() >= 0
                ? options.getDeleteInterval()
                : 5)
            * 1000;

    PCollection<String> dlqRecords =
        reshuffledRows.apply(
            "BatchAndWrite",
            new BatchAndWrite(
                options.getSinkType(),
                options.getSinkOptions(),
                options.getBatchSize(),
                options.getJdbcPoolSize(),
                updateIntervalMs,
                deleteIntervalMs,
                schemaView));

    if (options.getDlqDirectory() != null && !options.getDlqDirectory().isEmpty()) {
      dlqRecords.apply(
          "WriteDLQRecordsToGCS",
          com.google.cloud.teleport.v2.transforms.DLQWriteTransform.WriteDLQ.newBuilder()
              .withDlqDirectory(options.getDlqDirectory())
              .withTmpDirectory(options.getDlqDirectory() + "/tmp")
              .setIncludePaneInfo(true)
              .build());
    }

    return pipeline.run();
  }
}
