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
package com.google.cloud.teleport.v2.templates.dofn;

import com.google.cloud.teleport.v2.templates.model.DataGeneratorColumn;
import com.google.cloud.teleport.v2.templates.model.DataGeneratorForeignKey;
import com.google.cloud.teleport.v2.templates.model.DataGeneratorSchema;
import com.google.cloud.teleport.v2.templates.model.DataGeneratorTable;
import com.google.cloud.teleport.v2.templates.utils.Constants;
import com.google.cloud.teleport.v2.templates.utils.DataGeneratorUtils;
import com.google.cloud.teleport.v2.templates.utils.FailureRecord;
import com.google.cloud.teleport.v2.templates.utils.SchemaUtils;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ThreadLocalRandom;
import net.datafaker.Faker;
import org.apache.beam.sdk.metrics.Counter;
import org.apache.beam.sdk.metrics.Metrics;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.state.MapState;
import org.apache.beam.sdk.state.Timer;
import org.apache.beam.sdk.state.ValueState;
import org.apache.beam.sdk.values.Row;
import org.joda.time.Instant;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Orchestrates schema tree traversal and synthesized value updates, scheduling lifecycle timer
 * events to match operational distribution patterns.
 */
public class DataGeneratorEngine implements Serializable {
  private static final Logger LOG = LoggerFactory.getLogger(DataGeneratorEngine.class);

  private final Integer updateInterval;
  private final Integer deleteInterval;
  private final Faker faker;

  private transient volatile DataGeneratorSchema schema;
  private transient volatile List<String> insertTopoOrder;
  private transient volatile Map<String, Integer> topoIndex;

  private final Counter insertsGenerated =
      Metrics.counter(DataGeneratorEngine.class, "insertsGenerated");
  private final Counter updatesGenerated =
      Metrics.counter(DataGeneratorEngine.class, "updatesGenerated");
  private final Counter deletesGenerated =
      Metrics.counter(DataGeneratorEngine.class, "deletesGenerated");
  private final Counter unresolvableFkChildrenDropped =
      Metrics.counter(DataGeneratorEngine.class, "unresolvableFkChildrenDropped");

  public DataGeneratorEngine(Integer updateInterval, Integer deleteInterval, Faker faker) {
    this.updateInterval = updateInterval;
    this.deleteInterval = deleteInterval;
    this.faker = faker;
  }

  public void processRecord(
      String tableName,
      Row row,
      MapState<Long, List<LifecycleEvent>> eventQueueState,
      ValueState<List<Long>> activeTimestamps,
      MapState<String, DataGeneratorTable> tableMapState,
      Timer eventTimer,
      DataGeneratorSchema loadedSchema,
      MutationBatcher batcher) {

    this.schema = loadedSchema;
    if (this.insertTopoOrder == null) {
      this.insertTopoOrder = SchemaUtils.buildInsertTopoOrder(loadedSchema);
      this.topoIndex = SchemaUtils.buildTopoIndex(this.insertTopoOrder);
    }

    DataGeneratorTable table = schema.tables().get(tableName);

    if (table == null) {
      Metrics.counter(DataGeneratorEngine.class, "tableNotFound_" + tableName).inc();
      return;
    }

    tableMapState.put(tableName, table);

    processTable(
        table,
        row,
        eventQueueState,
        activeTimestamps,
        tableMapState,
        eventTimer,
        /* forcedDeleteTimestamp= */ 0L,
        /* earliestAncestorDelete= */ Long.MAX_VALUE,
        new HashMap<>(),
        batcher);
  }

  public void processScheduledEvents(
      MapState<Long, List<LifecycleEvent>> eventQueueState,
      ValueState<List<Long>> activeTimestamps,
      MapState<String, DataGeneratorTable> tableMapState,
      Timer eventTimer,
      MutationBatcher batcher,
      List<String> pendingDlq) {

    List<Long> timestamps = activeTimestamps.read();
    if (timestamps == null || timestamps.isEmpty()) {
      return;
    }

    long now = System.currentTimeMillis();
    int firstFutureIdx = 0;
    for (Long ts : timestamps) {
      if (ts > now) {
        break;
      }
      List<LifecycleEvent> events = eventQueueState.get(ts).read();
      if (events != null) {
        for (LifecycleEvent event : orderEventsForTick(events, tableMapState)) {
          try {
            processEvent(event, tableMapState, batcher);
          } catch (Exception genError) {
            LOG.error(
                "Lifecycle event generation failed for table {} ({})",
                event.tableName,
                event.type,
                genError);
            Metrics.counter(DataGeneratorEngine.class, "generationFailures").inc();
            pendingDlq.add(FailureRecord.toJson(event.tableName, event.type, null, genError));
          }
        }
        eventQueueState.remove(ts);
      }
      firstFutureIdx++;
    }

    timestamps = new ArrayList<>(timestamps.subList(firstFutureIdx, timestamps.size()));
    activeTimestamps.write(timestamps);
    if (!timestamps.isEmpty()) {
      eventTimer.set(Instant.ofEpochMilli(timestamps.get(0)));
    }
  }

  private void processTable(
      DataGeneratorTable table,
      Row row,
      MapState<Long, List<LifecycleEvent>> eventQueueState,
      ValueState<List<Long>> activeTimestamps,
      MapState<String, DataGeneratorTable> tableMapState,
      Timer eventTimer,
      long forcedDeleteTimestamp,
      long earliestAncestorDelete,
      Map<String, Row> ancestorRows,
      MutationBatcher batcher) {

    String tableName = table.name();
    tableMapState.put(tableName, table);

    Row fullRow = RowAssembler.completeRow(table, row, faker);
    String resolvedShardId =
        fullRow.getSchema().hasField(Constants.SHARD_ID_COLUMN_NAME)
            ? fullRow.getString(Constants.SHARD_ID_COLUMN_NAME)
            : "shard0";
    insertsGenerated.inc();
    LinkedHashMap<String, Object> pkMap = RowAssembler.pkValuesOf(fullRow, table);

    Row reducedRow = null;
    if (!pkMap.isEmpty()) {
      reducedRow = RowAssembler.createReducedRow(fullRow, table);
    }

    batcher.bufferRow(
        tableName, fullRow, Constants.MUTATION_INSERT, table, resolvedShardId, insertTopoOrder);

    long now = System.currentTimeMillis();
    long deleteTimestamp = 0L;
    int numUpdates = 0;
    long upInterval =
        (this.updateInterval != null && this.updateInterval > 0)
            ? (this.updateInterval * 1000L)
            : 5000L;
    long delInterval =
        (this.deleteInterval != null && this.deleteInterval > 0)
            ? (this.deleteInterval * 1000L)
            : 5000L;

    if (!pkMap.isEmpty()) {
      int tableInsertQps = table.insertQps();
      int tableUpdateQps = table.updateQps();
      int tableDeleteQps = table.deleteQps();

      double updateRatio = tableInsertQps > 0 ? (double) tableUpdateQps / tableInsertQps : 0.0;
      double deleteRatio = tableInsertQps > 0 ? (double) tableDeleteQps / tableInsertQps : 0.0;

      numUpdates = (int) updateRatio;
      double fractionalUpdate = updateRatio - numUpdates;
      if (ThreadLocalRandom.current().nextDouble() < fractionalUpdate) {
        numUpdates++;
      }

      if (forcedDeleteTimestamp > 0) {
        deleteTimestamp = forcedDeleteTimestamp;
      } else if (ThreadLocalRandom.current().nextDouble() < deleteRatio) {
        deleteTimestamp = now + upInterval * numUpdates + delInterval;
      }

      long myDeleteBound = deleteTimestamp > 0 ? deleteTimestamp : Long.MAX_VALUE;
      long effectiveDeleteBound = Math.min(myDeleteBound, earliestAncestorDelete);
      if (effectiveDeleteBound < Long.MAX_VALUE && numUpdates > 0) {
        long budget = effectiveDeleteBound - now - delInterval;
        if (upInterval * numUpdates > budget) {
          long minIntervalMs = 10L;
          upInterval = budget > 0 ? Math.max(minIntervalMs, budget / numUpdates) : minIntervalMs;
          long maxUpdates = budget > 0 ? budget / upInterval : 0;
          numUpdates = (int) Math.min(numUpdates, Math.max(0, maxUpdates));
        }
      }
    }

    Map<String, Row> updatedAncestorRows = new HashMap<>(ancestorRows);
    updatedAncestorRows.put(tableName, fullRow);

    long childEarliestAncestorDelete =
        deleteTimestamp > 0
            ? Math.min(earliestAncestorDelete, deleteTimestamp)
            : earliestAncestorDelete;

    if (table.childTables() != null) {
      for (String childTableName : table.childTables()) {
        DataGeneratorTable childTable = schema.tables().get(childTableName);
        if (childTable == null) {
          Metrics.counter(DataGeneratorEngine.class, "childTableNotFound_" + childTableName).inc();
          continue;
        }
        generateAndWriteChildren(
            table,
            fullRow,
            childTable,
            eventQueueState,
            activeTimestamps,
            tableMapState,
            eventTimer,
            deleteTimestamp,
            childEarliestAncestorDelete,
            updatedAncestorRows,
            batcher);
      }
    }

    if (!pkMap.isEmpty()) {
      for (int i = 1; i <= numUpdates; i++) {
        scheduleEvent(
            now + upInterval * i,
            new LifecycleEvent(pkMap, Constants.MUTATION_UPDATE, tableName, reducedRow),
            eventQueueState,
            activeTimestamps,
            eventTimer);
      }
      if (deleteTimestamp > 0) {
        scheduleEvent(
            deleteTimestamp,
            new LifecycleEvent(pkMap, Constants.MUTATION_DELETE, tableName, reducedRow),
            eventQueueState,
            activeTimestamps,
            eventTimer);
      }
    }
  }

  private void generateAndWriteChildren(
      DataGeneratorTable parentTable,
      Row parentRow,
      DataGeneratorTable childTable,
      MapState<Long, List<LifecycleEvent>> eventQueueState,
      ValueState<List<Long>> activeTimestamps,
      MapState<String, DataGeneratorTable> tableMapState,
      Timer eventTimer,
      long forcedDeleteTimestamp,
      long earliestAncestorDelete,
      Map<String, Row> ancestorRows,
      MutationBatcher batcher) {

    double parentQps = Math.max(1, parentTable.insertQps());
    double childQps = childTable.insertQps();
    double ratio = childQps / parentQps;
    int numChildren = (int) ratio;
    if (faker.random().nextDouble() < (ratio - numChildren)) {
      numChildren++;
    }

    for (int i = 0; i < numChildren; i++) {
      Row childRow = generateChildRow(parentTable, parentRow, childTable, ancestorRows);
      if (childRow == null) {
        unresolvableFkChildrenDropped.inc();
        continue;
      }
      processTable(
          childTable,
          childRow,
          eventQueueState,
          activeTimestamps,
          tableMapState,
          eventTimer,
          forcedDeleteTimestamp,
          earliestAncestorDelete,
          ancestorRows,
          batcher);
    }
  }

  private Row generateChildRow(
      DataGeneratorTable parentTable,
      Row parentRow,
      DataGeneratorTable childTable,
      Map<String, Row> ancestorRows) {

    Map<String, Object> columnValues = new HashMap<>();
    boolean resolvedAllFks = true;

    if (childTable.foreignKeys() != null && !childTable.foreignKeys().isEmpty()) {
      for (DataGeneratorForeignKey fk : childTable.foreignKeys()) {
        Row source = ancestorRows.get(fk.referencedTable());
        if (source == null) {
          LOG.warn(
              "Cannot resolve FK {} from {} -> {}: target table is not in the ancestor chain.",
              fk.name(),
              childTable.name(),
              fk.referencedTable());
          resolvedAllFks = false;
          break;
        }
        for (int i = 0; i < fk.keyColumns().size(); i++) {
          String refCol = fk.referencedColumns().get(i);
          if (!source.getSchema().hasField(refCol)) {
            throw new IllegalStateException(
                String.format(
                    "Foreign key constraint '%s' references column '%s' on table '%s', but that column does not exist in the generated parent row schema. "
                        + "Available fields in parent schema: %s. Please verify column name spelling and letter-casing.",
                    fk.name(), refCol, fk.referencedTable(), source.getSchema().getFieldNames()));
          }
          columnValues.put(fk.keyColumns().get(i), source.getValue(refCol));
        }
      }
    }
    if (!resolvedAllFks) {
      return null;
    }

    boolean hasFks = childTable.foreignKeys() != null && !childTable.foreignKeys().isEmpty();
    if (!hasFks
        && childTable.interleavedInTable() != null
        && childTable.interleavedInTable().equals(parentTable.name())) {
      for (String pk : parentTable.primaryKeys()) {
        Object val = getFieldFromRow(parentRow, pk);
        if (val != null) {
          columnValues.put(pk, val);
        }
      }
    }

    Schema.Builder schemaBuilder = Schema.builder();
    List<Object> values = new ArrayList<>();

    for (DataGeneratorColumn col : childTable.columns()) {
      if (col.skip()) {
        continue;
      }
      Object val;
      if (columnValues.containsKey(col.name())) {
        val = columnValues.get(col.name());
      } else {
        val = DataGeneratorUtils.generateValue(col, faker);
      }
      schemaBuilder.addField(
          Schema.Field.of(col.name(), DataGeneratorUtils.mapToBeamFieldType(col.logicalType())));
      values.add(val);
    }

    String shardId =
        parentRow.getSchema().hasField(Constants.SHARD_ID_COLUMN_NAME)
            ? parentRow.getString(Constants.SHARD_ID_COLUMN_NAME)
            : null;
    if (shardId != null) {
      schemaBuilder.addField(
          Schema.Field.of(Constants.SHARD_ID_COLUMN_NAME, Schema.FieldType.STRING));
      values.add(shardId);
    }
    return Row.withSchema(schemaBuilder.build()).addValues(values).build();
  }

  private Object getFieldFromRow(Row row, String fieldName) {
    try {
      return row.getValue(fieldName);
    } catch (IllegalArgumentException e) {
      return null;
    }
  }

  private void scheduleEvent(
      long timestamp,
      LifecycleEvent event,
      MapState<Long, List<LifecycleEvent>> eventQueueState,
      ValueState<List<Long>> activeTimestamps,
      Timer eventTimer) {

    long snappedTimestamp = (timestamp / 1000) * 1000;

    List<LifecycleEvent> events = eventQueueState.get(snappedTimestamp).read();
    if (events == null) {
      events = new ArrayList<>();
    }
    events.add(event);
    eventQueueState.put(snappedTimestamp, events);

    List<Long> timestamps = activeTimestamps.read();
    if (timestamps == null) {
      timestamps = new ArrayList<>();
    }
    int idx = Collections.binarySearch(timestamps, snappedTimestamp);
    if (idx < 0) {
      timestamps.add(-(idx + 1), snappedTimestamp);
      activeTimestamps.write(timestamps);
    }
    eventTimer.set(Instant.ofEpochMilli(timestamps.get(0)));
  }

  private List<LifecycleEvent> orderEventsForTick(
      List<LifecycleEvent> events, MapState<String, DataGeneratorTable> tableMapState) {
    Map<String, Integer> depthCache = new HashMap<>();
    Comparator<LifecycleEvent> cmp =
        (a, b) -> {
          int priA = isDelete(a) ? 1 : 0;
          int priB = isDelete(b) ? 1 : 0;
          if (priA != priB) {
            return Integer.compare(priA, priB);
          }
          int depthA = depthCache.computeIfAbsent(a.tableName, t -> depthOf(t, tableMapState));
          int depthB = depthCache.computeIfAbsent(b.tableName, t -> depthOf(t, tableMapState));
          return isDelete(a) ? Integer.compare(depthB, depthA) : Integer.compare(depthA, depthB);
        };
    List<LifecycleEvent> copy = new ArrayList<>(events);
    copy.sort(cmp);
    return copy;
  }

  private boolean isDelete(LifecycleEvent e) {
    return Constants.MUTATION_DELETE.equals(e.type);
  }

  private int depthOf(String tableName, MapState<String, DataGeneratorTable> tableMapState) {
    if (topoIndex != null && topoIndex.containsKey(tableName)) {
      return topoIndex.get(tableName);
    }
    if (schema != null && insertTopoOrder == null) {
      insertTopoOrder = SchemaUtils.buildInsertTopoOrder(schema);
      topoIndex = SchemaUtils.buildTopoIndex(insertTopoOrder);
      Integer idx = topoIndex.get(tableName);
      return idx == null ? 0 : idx;
    }
    return 0;
  }

  private void processEvent(
      LifecycleEvent event,
      MapState<String, DataGeneratorTable> tableMapState,
      MutationBatcher batcher)
      throws Exception {

    DataGeneratorTable table = tableMapState.get(event.tableName).read();
    if (table == null) {
      return;
    }

    Row originalRow = event.reducedRow;
    String shardId = "";
    if (originalRow != null && originalRow.getSchema().hasField(Constants.SHARD_ID_COLUMN_NAME)) {
      shardId = originalRow.getString(Constants.SHARD_ID_COLUMN_NAME);
    }

    if (Constants.MUTATION_UPDATE.equals(event.type)) {
      if (originalRow == null) {
        LOG.warn("Cannot process update for table {} without original row state.", event.tableName);
        return;
      }
      Row updateRow = RowAssembler.generateUpdateRow(event.pkValues, table, originalRow, faker);
      batcher.bufferRow(
          event.tableName, updateRow, Constants.MUTATION_UPDATE, table, shardId, insertTopoOrder);
      updatesGenerated.inc();
    } else if (Constants.MUTATION_DELETE.equals(event.type)) {
      Row deleteRow = RowAssembler.generateDeleteRow(event.pkValues, table);
      batcher.bufferRow(
          event.tableName, deleteRow, Constants.MUTATION_DELETE, table, shardId, insertTopoOrder);
      deletesGenerated.inc();
    }
  }
}
