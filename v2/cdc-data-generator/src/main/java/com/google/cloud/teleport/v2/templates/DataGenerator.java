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

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.cloud.teleport.metadata.Template;
import com.google.cloud.teleport.metadata.TemplateCategory;
import com.google.cloud.teleport.v2.common.UncaughtExceptionLogger;
import com.google.cloud.teleport.v2.templates.model.DataGeneratorSchema;
import com.google.cloud.teleport.v2.templates.model.DataGeneratorTable;
import com.google.cloud.teleport.v2.templates.model.GeneratedRecord;
import com.google.cloud.teleport.v2.templates.model.SchemaConfig;
import com.google.cloud.teleport.v2.templates.transforms.BatchAndWrite;
import com.google.cloud.teleport.v2.templates.transforms.GeneratePrimaryKey;
import com.google.cloud.teleport.v2.templates.transforms.GenerateTicks;
import com.google.cloud.teleport.v2.templates.transforms.SchemaLoader;
import com.google.cloud.teleport.v2.templates.transforms.SelectTable;
import com.google.common.io.CharStreams;
import com.jasonclawson.jackson.dataformat.hocon.HoconFactory;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.Reader;
import java.nio.channels.Channels;
import java.nio.channels.ReadableByteChannel;
import java.nio.charset.StandardCharsets;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.PipelineResult;
import org.apache.beam.sdk.io.FileSystems;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.Reshuffle;
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

    // Register SerializableCoder for Row class globally since we use dynamically constructed
    // schemas at runtime
    pipeline
        .getCoderRegistry()
        .registerCoderForClass(
            Row.class, org.apache.beam.sdk.coders.SerializableCoder.of(Row.class));

    SchemaConfig schemaConfig = null;
    if (options.getSchemaConfig() != null && !options.getSchemaConfig().isEmpty()) {
      try (ReadableByteChannel channel =
          FileSystems.open(FileSystems.matchNewResource(options.getSchemaConfig(), false))) {
        try (Reader reader =
            new InputStreamReader(Channels.newInputStream(channel), StandardCharsets.UTF_8)) {
          String content = CharStreams.toString(reader);
          ObjectMapper mapper = new ObjectMapper(new HoconFactory());
          schemaConfig = mapper.readValue(content, SchemaConfig.class);
        }
      } catch (IOException e) {
        throw new RuntimeException(
            "Failed to read schema config from " + options.getSchemaConfig(), e);
      }
    }

    PCollectionView<DataGeneratorSchema> schemaView =
        pipeline.apply(
            "LoadSchema",
            new SchemaLoader(
                options.getSinkType(),
                options.getSinkOptions(),
                options.getInsertQps(),
                options.getUpdateQps(),
                options.getDeleteQps(),
                schemaConfig));

    // Generate ticks based on schema QPS
    PCollection<DataGeneratorTable> ticks =
        pipeline
            .apply(
                "TriggerTick",
                org.apache.beam.sdk.transforms.PeriodicImpulse.create()
                    .withInterval(org.joda.time.Duration.standardSeconds(1)))
            .apply("GenerateTicks", new GenerateTicks(schemaView))
            .apply("ReshuffleProvider", org.apache.beam.sdk.transforms.Reshuffle.viaRandomKey())
            .apply("SelectTable", new SelectTable(schemaView));

    // Generate Primary Keys
    int maxShards = options.getMaxShards() != null ? options.getMaxShards() : 1;
    PCollection<KV<String, Row>> pendingRows =
        ticks.apply(
            "GeneratePrimaryKey",
            new GeneratePrimaryKey(
                maxShards, options.getSinkOptions(), options.getSinkType().name()));

    // Reshuffle based on Hash(TableName + PK) to ensure same PK goes to same worker
    // Key = Hash(TableName + PK) % 5000
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
                        int shard = Math.abs(hash % 5000);
                        c.output(KV.of(shard, GeneratedRecord.create(tableName, pkValues)));
                      }
                    }))
            .apply("Reshuffle", Reshuffle.of());

    PCollection<String> dlqRecords =
        reshuffledRows.apply(
            "BatchAndWrite",
            new BatchAndWrite(
                options.getSinkType(),
                options.getSinkOptions(),
                options.getBatchSize(),
                options.getJdbcPoolSize(),
                options.getUpdateInterval(),
                options.getDeleteInterval(),
                schemaView));

    if (options.getDlqDirectory() != null && !options.getDlqDirectory().isEmpty()) {
      dlqRecords.apply(
          "WriteDLQRecordsToGCS",
          com.google.cloud.teleport.v2.transforms.DLQWriteTransform.WriteDLQ.newBuilder()
              .withDlqDirectory(options.getDlqDirectory())
              .withTmpDirectory(options.getDlqDirectory() + "/tmp")
              .build());
    }

    return pipeline.run();
  }
}
