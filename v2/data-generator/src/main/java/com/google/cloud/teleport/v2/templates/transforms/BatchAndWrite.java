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

import com.github.javafaker.Faker;
import com.google.cloud.teleport.v2.spanner.migrations.shard.Shard;
import com.google.cloud.teleport.v2.spanner.migrations.utils.SecretManagerAccessorImpl;
import com.google.cloud.teleport.v2.spanner.migrations.utils.ShardFileReader;
import com.google.cloud.teleport.v2.templates.DataGeneratorOptions;
import com.google.cloud.teleport.v2.templates.model.DataGeneratorColumn;
import com.google.cloud.teleport.v2.templates.model.DataGeneratorForeignKey;
import com.google.cloud.teleport.v2.templates.model.DataGeneratorSchema;
import com.google.cloud.teleport.v2.templates.model.DataGeneratorTable;
import com.google.cloud.teleport.v2.templates.utils.DataGeneratorUtils;
import com.google.cloud.teleport.v2.templates.writer.DataWriter;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import org.apache.beam.sdk.metrics.Counter;
import org.apache.beam.sdk.metrics.Metrics;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionView;
import org.apache.beam.sdk.values.PDone;
import org.apache.beam.sdk.values.Row;

/**
 * A {@link PTransform} that generates the remaining columns for a record, batches them, and writes
 * to the sink.
 */
public class BatchAndWrite extends PTransform<PCollection<KV<String, Row>>, PDone> {

  private final String sinkType;
  private final String sinkOptionsPath;
  private final Integer batchSize;
  private final PCollectionView<DataGeneratorSchema> schemaView;

  public BatchAndWrite(
      String sinkType,
      String sinkOptionsPath,
      Integer batchSize,
      PCollectionView<DataGeneratorSchema> schemaView) {
    this.sinkType = sinkType;
    this.sinkOptionsPath = sinkOptionsPath;
    this.batchSize = batchSize;
    this.schemaView = schemaView;
  }

  @Override
  public PDone expand(PCollection<KV<String, Row>> input) {
    input.apply(
        "BatchAndWriteFn",
        ParDo.of(new BatchAndWriteFn(sinkType, sinkOptionsPath, batchSize, schemaView))
            .withSideInputs(schemaView));
    return PDone.in(input.getPipeline());
  }

  static class BatchAndWriteFn extends DoFn<KV<String, Row>, Void> {
    private final String sinkType;
    private final String sinkOptionsPath;
    private final Integer configuredBatchSize;
    private final PCollectionView<DataGeneratorSchema> schemaView;
    private final int batchSize;
    protected transient DataWriter writer;
    protected transient Faker faker;
    private transient DataGeneratorSchema schema;
    private transient List<String> logicalShardIds;
    // Buffer: TableName -> List of Rows
    private transient Map<String, List<Row>> buffers;
    // Map to keep track of DataGeneratorTable objects for flushing
    private transient Map<String, DataGeneratorTable> tableMap;

    private final Counter insertsGenerated =
        Metrics.counter(BatchAndWriteFn.class, "insertsGenerated");
    private final Counter batchesWritten = Metrics.counter(BatchAndWriteFn.class, "batchesWritten");
    private final Counter recordsWritten = Metrics.counter(BatchAndWriteFn.class, "recordsWritten");

    public BatchAndWriteFn(
        DataGeneratorOptions options, PCollectionView<DataGeneratorSchema> schemaView) {
      this.sinkType = options.getSinkType().name();
      this.sinkOptionsPath = options.getSinkOptions();
      this.configuredBatchSize = options.getBatchSize();
      this.schemaView = schemaView;
      this.schema = null;
      this.batchSize = options.getBatchSize() != null ? options.getBatchSize() : 100;
    }

    public BatchAndWriteFn(
        String sinkType,
        String sinkOptionsPath,
        Integer batchSize,
        PCollectionView<DataGeneratorSchema> schemaView) {
      this.sinkType = sinkType;
      this.sinkOptionsPath = sinkOptionsPath;
      this.configuredBatchSize = batchSize;
      this.schemaView = schemaView;
      this.schema = null;
      this.batchSize = batchSize != null ? batchSize : 100;
    }

    @Setup
    public void setup() {
      if (this.writer == null) {
        String sinkOptionsJson = readSinkOptions(sinkOptionsPath);
        if ("MYSQL".equalsIgnoreCase(sinkType)) {
          this.writer =
              new com.google.cloud.teleport.v2.templates.writer.MySqlDataWriter(sinkOptionsJson);
          try {
            ShardFileReader shardFileReader = new ShardFileReader(new SecretManagerAccessorImpl());
            List<Shard> shards = shardFileReader.getOrderedShardDetails(sinkOptionsPath);
            this.logicalShardIds =
                shards.stream().map(Shard::getLogicalShardId).collect(Collectors.toList());
          } catch (Exception e) {
            throw new RuntimeException("Failed to read shards from " + sinkOptionsPath, e);
          }
        } else {
          this.writer =
              new com.google.cloud.teleport.v2.templates.writer.SpannerDataWriter(sinkOptionsJson);
        }
      }
      if (this.faker == null) {
        this.faker = new Faker();
      }
    }

    private String readSinkOptions(String path) {
      try (java.nio.channels.ReadableByteChannel channel =
          org.apache.beam.sdk.io.FileSystems.open(
              org.apache.beam.sdk.io.FileSystems.matchNewResource(path, false))) {
        try (java.io.Reader reader =
            new java.io.InputStreamReader(
                java.nio.channels.Channels.newInputStream(channel),
                java.nio.charset.StandardCharsets.UTF_8)) {
          return com.google.common.io.CharStreams.toString(reader);
        }
      } catch (java.io.IOException e) {
        throw new RuntimeException("Failed to read sink options from " + path, e);
      }
    }

    @StartBundle
    public void startBundle() {
      System.out.println("StartBundle called");
      this.buffers = new HashMap<>();
      this.tableMap = new HashMap<>();
    }

    @ProcessElement
    public void processElement(ProcessContext c) {
      if (schema == null) {
        schema = c.sideInput(schemaView);
      }
      String tableName = c.element().getKey();
      Row row = c.element().getValue();
      DataGeneratorTable table = schema.tables().get(tableName);

      if (table == null) {
        // LOG ERROR or add to DLQ
        Metrics.counter(BatchAndWriteFn.class, "tableNotFound_" + tableName).inc();
        return;
      }

      // Recursively process this table and its children
      processTable(table, row);
    }

    /**
     * Processes a single table record: generates mutation, buffers/writes it, and recursively
     * processes children.
     */
    private void processTable(DataGeneratorTable table, Row row) {
      insertsGenerated.inc();
      String tableName = table.name();
      tableMap.putIfAbsent(tableName, table);

      // 0. Ensure Row has all columns (generate missing if needed)
      Row fullRow = completeRow(table, row);

      // 1. Buffer Row for current record
      String shardId = "";
      if ("MYSQL".equalsIgnoreCase(sinkType)
          && logicalShardIds != null
          && !logicalShardIds.isEmpty()) {
        shardId =
            logicalShardIds.get(
                java.util.concurrent.ThreadLocalRandom.current().nextInt(logicalShardIds.size()));
      }
      String bufferKey = shardId.isEmpty() ? tableName : tableName + "#" + shardId;

      buffers.computeIfAbsent(bufferKey, k -> new ArrayList<>()).add(fullRow);
      if (buffers.get(bufferKey).size() >= batchSize) {
        flush(bufferKey);
      }

      // 2. Process Children (Cascading Generation)
      if (table.children() != null && !table.children().isEmpty()) {
        for (String childTableName : table.children()) {
          DataGeneratorTable childTable = schema.tables().get(childTableName);
          if (childTable != null) {
            generateAndWriteChildren(table, fullRow, childTable);
          } else {
            // Should not happen if schema DAG is built correctly
            Metrics.counter(BatchAndWriteFn.class, "childTableNotFound_" + childTableName).inc();
          }
        }
      }
    }

    private void generateAndWriteChildren(
        DataGeneratorTable parentTable, Row parentRow, DataGeneratorTable childTable) {
      // Calculate number of children to generate based on QPS ratio
      // Avoid division by zero
      double parentQps = Math.max(1, parentTable.qps());
      double childQps = childTable.qps();
      double ratio = childQps / parentQps;

      int numChildren = (int) ratio;
      // Probabilistic rounding for fractional ratio
      if (faker.random().nextDouble() < (ratio - numChildren)) {
        numChildren++;
      }

      for (int i = 0; i < numChildren; i++) {
        Row childRow = generateChildRow(parentTable, parentRow, childTable);
        processTable(childTable, childRow);
      }
    }

    private Row generateChildRow(
        DataGeneratorTable parentTable, Row parentRow, DataGeneratorTable childTable) {
      // 1. Determine values for all columns
      // Foreign Keys must match Parent
      // Other columns generated via Faker

      Map<String, Object> columnValues = new HashMap<>();

      // A. Populate FKs from Parent
      // Find the ForeignKey definition that points to parentTable
      // We assume SchemaUtils.setSchemaDAG has set up the parent-child relationship
      // correctly
      // such that 'childTable' is indeed a child of 'parentTable'.
      // There might be multiple FKs? We try to match one.

      boolean fkFound = false;
      if (childTable.foreignKeys() != null) {
        for (DataGeneratorForeignKey fk : childTable.foreignKeys()) {
          if (fk.referencedTable().equals(parentTable.name())) {
            // Map values
            for (int i = 0; i < fk.keyColumns().size(); i++) {
              String childCol = fk.keyColumns().get(i);
              String parentCol = fk.referencedColumns().get(i);

              // Get value from parentRow
              // parentRow might verify schema?
              Object val = getFieldFromRow(parentRow, parentCol);
              columnValues.put(childCol, val);
            }
            fkFound = true;
            // We only satisfy ONE parent FK (the driving one).
            // If there are others, they will be random (and potentially invalid if not
            // carefully designed).
            break;
          }
        }
      }

      // Also check Interleaved tables (if they don't have explicit FK constraint
      // object, typical in Spanner DDL parsing?)
      // SpannerSchemaFetcher usually creates FK objects for interleaved tables too?
      // If not, we might need logic here.
      // Assuming FK object exists for now as per widespread Spanner usage or
      // SchemaUtils logic.
      if (!fkFound
          && childTable.interleavedInTable() != null
          && childTable.interleavedInTable().equals(parentTable.name())) {
        // Interleaved usually implies sharing PK prefix.
        // We should copy PKs from parent to child's PK prefix.
        // Assumption: Child PK starts with Parent PK columns in same order/name?
        // Or we utilize the fact that we have the values.
        // Let's rely on column name matching for Interleaved if no explicit FK.
        for (String pk : parentTable.primaryKeys()) {
          Object val = getFieldFromRow(parentRow, pk);
          if (val != null) {
            columnValues.put(pk, val);
          }
        }
      }

      // B. Generate remaining columns
      Schema.Builder schemaBuilder = Schema.builder();
      List<Object> values = new ArrayList<>();

      for (DataGeneratorColumn col : childTable.columns()) {
        Object val = columnValues.get(col.name());
        if (val == null) {
          val = generateValue(col);
        }

        // Map to Beam Schema Type
        schemaBuilder.addField(
            Schema.Field.of(col.name(), DataGeneratorUtils.mapToBeamFieldType(col.logicalType())));
        values.add(val);
      }

      return Row.withSchema(schemaBuilder.build()).addValues(values).build();
    }

    private Object getFieldFromRow(Row row, String fieldName) {
      // Row might not have this field?
      // Row schema in GeneratePrimaryKey only has PKs!
      // Wait. 'parentRow' coming from GeneratePrimaryKey ONLY has PK columns.
      // If the FK references a NON-PK column in parent, we are in trouble.
      // BUT standard parent-child relationship usually refs PK.
      // If it refs non-PK, we don't have that value because GeneratePrimaryKey only
      // generated PKs.
      // We *extended* the Row in 'processTable -> generateMutation'? No, we just
      // built mutation.
      // We need 'parentRow' to contain ALL columns if we cascade?
      // OR we just generate all columns for Parent at the same time?
      // Currently `BatchAndWrite` generates non-PK columns inside `generateMutation`.
      // These values are NOT saved in the Row object being passed around.
      // FIX: We must generate the FULL Row for the parent *before* processing
      // children.
      //
      // However, `input` to MatchAndWrite is KV<Table, Row(PKs)>.
      // inside processElement, we generate other columns.
      // We should reconstruct a FULL Row including generated columns to pass to
      // children.

      try {
        return row.getValue(fieldName);
      } catch (IllegalArgumentException e) {
        // Field not found. This effectively means we can't propagate this value.
        // If it's a generated non-PK column, we missed it.
        return null;
      }
    }

    @FinishBundle
    public void finishBundle() {
      System.out.println("FinishBundle called");
      for (String bufferKey : buffers.keySet()) {
        if (!buffers.get(bufferKey).isEmpty()) {
          flush(bufferKey);
        }
      }
    }

    @Teardown
    public void teardown() {
      if (writer != null) {
        try {
          writer.close();
        } catch (Exception e) {
          throw new RuntimeException("Failed to close writer", e);
        }
      }
    }

    private void flush(String bufferKey) {
      String[] parts = bufferKey.split("#");
      String tableName = parts[0];
      String shardId = parts.length > 1 ? parts[1] : "";
      List<Row> batch = buffers.get(bufferKey);
      if (batch != null && !batch.isEmpty()) {
        if ("MYSQL".equalsIgnoreCase(sinkType)) {
          ((com.google.cloud.teleport.v2.templates.writer.MySqlDataWriter) writer)
              .write(batch, tableMap.get(tableName), shardId);
        } else {
          writer.write(batch, tableMap.get(tableName));
        }
        batchesWritten.inc();
        recordsWritten.inc(batch.size());
        if (!shardId.isEmpty()) {
          Metrics.counter(BatchAndWriteFn.class, "recordsWritten_" + shardId).inc(batch.size());
        }
        batch.clear();
      }
    }

    // Removed generateMutation and setColumnValue methods to SpannerDataWriter
    // Note: This returns Mutation. It does NOT return the generated values.
    // This is a problem for recursion if children need those generated values.
    // Better: modify processTable to:
    // 1. Complete the Row (generate missing fields).
    // 2. Build Mutation from Completed Row.
    // 3. Pass Completed Row to children.

    private Row completeRow(DataGeneratorTable table, Row partialRow) {
      // If partialRow has all columns, return it.
      if (partialRow.getSchema().getFieldCount() == table.columns().size()) {
        return partialRow;
      }

      Map<String, Object> currentValues = new HashMap<>();
      for (Schema.Field field : partialRow.getSchema().getFields()) {
        currentValues.put(field.getName(), partialRow.getValue(field.getName()));
      }

      List<Object> values = new ArrayList<>();
      Schema.Builder schemaBuilder = Schema.builder();

      for (DataGeneratorColumn col : table.columns()) {
        Object val = currentValues.get(col.name());
        if (val == null) {
          val = generateValue(col);
          // Store so we can use it? Not needed for Map, but needed for Row.
        }
        schemaBuilder.addField(
            Schema.Field.of(col.name(), DataGeneratorUtils.mapToBeamFieldType(col.logicalType())));
        values.add(val);
      }
      return Row.withSchema(schemaBuilder.build()).addValues(values).build();
    }

    private Object generateValue(DataGeneratorColumn column) {
      return DataGeneratorUtils.generateValue(column, faker);
    }

    // Removed mapToFieldType as it is now in DataGeneratorUtils
  }
}
