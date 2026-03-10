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
import com.google.cloud.ByteArray;
import com.google.cloud.Timestamp;
import com.google.cloud.spanner.Mutation;
import com.google.cloud.spanner.Value;
import com.google.cloud.teleport.v2.templates.DataGeneratorOptions;
import com.google.cloud.teleport.v2.templates.common.DataGeneratorAbstractions.DataGeneratorColumn;
import com.google.cloud.teleport.v2.templates.common.DataGeneratorAbstractions.DataGeneratorForeignKey;
import com.google.cloud.teleport.v2.templates.common.DataGeneratorAbstractions.DataGeneratorTable;
import com.google.cloud.teleport.v2.templates.common.DataGeneratorAbstractions.LogicalType;
import com.google.cloud.teleport.v2.templates.common.DataGeneratorAbstractions.Row;
import com.google.cloud.teleport.v2.templates.model.DataGeneratorColumn;
import com.google.cloud.teleport.v2.templates.model.DataGeneratorForeignKey;
import com.google.cloud.teleport.v2.templates.model.LogicalType;
import com.google.cloud.teleport.v2.templates.utils.DataGeneratorUtils;
import com.google.cloud.teleport.v2.templates.writer.DataWriter;
import com.google.cloud.teleport.v2.templates.writer.SpannerDataWriter;
import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
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
import org.joda.time.Instant;

/**
 * A {@link PTransform} that generates the remaining columns for a record, batches them, and writes
 * to the sink.
 */
public class BatchAndWrite extends PTransform<PCollection<KV<String, Row>>, PDone> {

  private final String sinkOptionsPath;
  private final Integer batchSize;
  private final PCollectionView<Map<String, DataGeneratorTable>> schemaView;

  public BatchAndWrite(
      String sinkOptionsPath,
      Integer batchSize,
      PCollectionView<Map<String, DataGeneratorTable>> schemaView) {
    this.sinkOptionsPath = sinkOptionsPath;
    this.batchSize = batchSize;
    this.schemaView = schemaView;
  }

  @Override
  public PDone expand(PCollection<KV<String, Row>> input) {
    input.apply(
        "BatchAndWriteFn",
        ParDo.of(new BatchAndWriteFn(sinkOptionsPath, batchSize, schemaView))
            .withSideInputs(schemaView));
    return PDone.in(input.getPipeline());
  }

  static class BatchAndWriteFn extends DoFn<KV<String, Row>, Void> {
    private final String sinkOptionsPath;
    private final Integer configuredBatchSize;
    private final PCollectionView<Map<String, DataGeneratorTable>> schemaView;
    private final int batchSize;
    protected transient DataWriter writer;
    protected transient Faker faker;
    private transient Map<String, DataGeneratorTable> schemaMap;
    // Buffer: TableName -> List of Mutations
    private transient Map<String, List<Mutation>> buffers;
    // Map to keep track of DataGeneratorTable objects for flushing
    private transient Map<String, DataGeneratorTable> tableMap;

    private final Counter insertsGenerated =
        Metrics.counter(BatchAndWriteFn.class, "insertsGenerated");
    private final Counter batchesWritten = Metrics.counter(BatchAndWriteFn.class, "batchesWritten");
    private final Counter recordsWritten = Metrics.counter(BatchAndWriteFn.class, "recordsWritten");

    public BatchAndWriteFn(
        DataGeneratorOptions options, PCollectionView<Map<String, DataGeneratorTable>> schemaView) {
      this.sinkOptionsPath = options.getSinkOptions();
      this.configuredBatchSize = options.getBatchSize();
      this.schemaView = schemaView;
      this.schemaMap = null;
      this.batchSize = options.getBatchSize() != null ? options.getBatchSize() : 100;
    }

    public BatchAndWriteFn(
        String sinkOptionsPath,
        Integer batchSize,
        PCollectionView<Map<String, DataGeneratorTable>> schemaView) {
      this.sinkOptionsPath = sinkOptionsPath;
      this.configuredBatchSize = batchSize;
      this.schemaView = schemaView;
      this.schemaMap = null;
      this.batchSize = batchSize != null ? batchSize : 100;
    }

    @Setup
    public void setup() {
      if (this.writer == null) {
        String sinkOptionsJson = readSinkOptions(sinkOptionsPath);
        // SpannerDataWriter seems to only need project, instance, and database from
        // options.
        // Create a minimal options object for it.
        this.writer = new SpannerDataWriter(sinkOptionsJson);
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
      if (schemaMap == null) {
        schemaMap = c.sideInput(schemaView);
      }
      String tableName = c.element().getKey();
      Row row = c.element().getValue();
      DataGeneratorTable table = schemaMap.get(tableName);

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

      // 1. Generate and Buffer Mutation for current record
      Mutation mutation = rowToMutation(table, fullRow);
      buffers.computeIfAbsent(tableName, k -> new ArrayList<>()).add(mutation);

      if (buffers.get(tableName).size() >= batchSize) {
        flush(tableName);
      }

      // 2. Process Children (Cascading Generation)
      if (table.children() != null && !table.children().isEmpty()) {
        for (String childTableName : table.children()) {
          DataGeneratorTable childTable = schemaMap.get(childTableName);
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
      for (String tableName : buffers.keySet()) {
        if (!buffers.get(tableName).isEmpty()) {
          flush(tableName);
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

    private void flush(String tableName) {
      List<Mutation> batch = buffers.get(tableName);
      if (batch != null && !batch.isEmpty()) {
        System.out.println("Flushing table: " + tableName + ", size: " + batch.size());
        writer.write(batch, tableMap.get(tableName));
        batchesWritten.inc();
        recordsWritten.inc(batch.size());
        batch.clear();
      }
    }

    private Mutation generateMutation(DataGeneratorTable table, Row row) {
      Mutation.WriteBuilder builder = Mutation.newInsertOrUpdateBuilder(table.name());

      // Row 'row' might be Partial (PK only) OR Full (if recursively generated).
      // We need to handle both.
      // Actually, for recursion to work, we probably want 'row' to be FULL row
      // always?
      // But for the input from GeneratePrimaryKey, it is Partial.
      // So we must fill in the gaps AND update the 'row' object if we want to pass it
      // to children?
      // But Row is immutable.

      // Let's split this:
      // 1. Identify which columns are present in 'row'.
      // 2. Generate missing columns.
      // 3. Build Mutation.
      // 4. (Implicitly) we have the full set of values now.

      // To pass to children, 'processTable' should probably take the 'full' set of
      // values.
      // But 'processTable' takes 'Row'.
      // Let's refactor 'processTable' to ensure it constructs a Full Row if partial
      // is passed?

      // Wait, 'generateChildRow' needs Full Parent Row if FK points to non-PK.
      // If FK points to PK (99% cases), Partial Row is fine.
      // Let's assume FK points to PK for now to keep it simple, OR reconstruct full
      // row.

      return rowToMutation(table, row);
    }

    // Helper to convert Row to Mutation, generating missing fields on the fly
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

    private Mutation rowToMutation(DataGeneratorTable table, Row row) {
      Mutation.WriteBuilder builder = Mutation.newInsertOrUpdateBuilder(table.name());
      for (DataGeneratorColumn col : table.columns()) {
        Object val = getFieldFromRow(row, col.name());
        setColumnValueFromLogicalType(builder, col.name(), val, col.logicalType());
      }
      return builder.build();
    }

    private void setColumnValue(
        Mutation.WriteBuilder builder,
        String columnName,
        Object value,
        org.apache.beam.sdk.schemas.Schema.FieldType splitType) {

      // Deprecated or unused if we use setColumnValueFromLogicalType
      if (value == null) {
        return;
      }
      // implementation ... (omitting as we will use the logic below)
    }

    private void setColumnValueFromLogicalType(
        Mutation.WriteBuilder builder, String columnName, Object value, LogicalType logicalType) {
      if (value == null) {
        return;
      }
      switch (logicalType) {
        case STRING:
        case JSON:
          builder.set(columnName).to((String) value);
          break;
        case INT64:
          builder.set(columnName).to((Long) value);
          break;
        case FLOAT64:
          builder.set(columnName).to((Double) value);
          break;
        case NUMERIC:
          builder.set(columnName).to((BigDecimal) value);
          break;
        case BOOLEAN:
          builder.set(columnName).to((Boolean) value);
          break;
        case BYTES:
          builder.set(columnName).to(Value.bytes(ByteArray.copyFrom((byte[]) value)));
          break;
        case TIMESTAMP:
        case DATE:
          // Value could be Instant or Long? Faker generates Instant.
          if (value instanceof Instant) {
            builder
                .set(columnName)
                .to(Timestamp.ofTimeMicroseconds(((Instant) value).getMillis() * 1000));
          } else {
            builder.set(columnName).to(value.toString());
          }

          break;
        default:
          builder.set(columnName).to(value.toString());
      }
    }

    private Object generateValue(DataGeneratorColumn column) {
      return DataGeneratorUtils.generateValue(column, faker);
    }

    // Removed mapToFieldType as it is now in DataGeneratorUtils
  }
}
