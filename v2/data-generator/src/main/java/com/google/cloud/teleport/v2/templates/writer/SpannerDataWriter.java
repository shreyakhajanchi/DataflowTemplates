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
package com.google.cloud.teleport.v2.templates.writer;

import com.google.cloud.ByteArray;
import com.google.cloud.spanner.Mutation;
import com.google.cloud.spanner.Value;
import com.google.cloud.teleport.v2.templates.DataGeneratorOptions;
import com.google.cloud.teleport.v2.templates.model.DataGeneratorColumn;
import com.google.cloud.teleport.v2.templates.model.DataGeneratorTable;
import com.google.cloud.teleport.v2.templates.model.LogicalType;
import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.List;
import org.apache.beam.sdk.io.gcp.spanner.SpannerAccessor;
import org.apache.beam.sdk.io.gcp.spanner.SpannerConfig;
import org.apache.beam.sdk.values.Row;
import org.joda.time.Instant;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Spanner implementation of {@link DataWriter}. */
public class SpannerDataWriter implements DataWriter {
  private static final Logger LOG = LoggerFactory.getLogger(SpannerDataWriter.class);
  private final SpannerConfig spannerConfig;
  private transient SpannerAccessor spannerAccessor;

  public SpannerDataWriter(DataGeneratorOptions options, String sinkOptionsJson) {
    this(sinkOptionsJson);
  }

  public SpannerDataWriter(String sinkOptionsJson) {
    this.spannerConfig = parseSpannerConfig(sinkOptionsJson);
  }

  private SpannerConfig parseSpannerConfig(String sinkOptionsJson) {
    org.json.JSONObject json = new org.json.JSONObject(sinkOptionsJson);
    String instanceId = json.getString("instanceId");
    String databaseId = json.getString("databaseId");
    String projectId = json.getString("projectId");

    return SpannerConfig.create()
        .withProjectId(projectId)
        .withInstanceId(instanceId)
        .withDatabaseId(databaseId);
  }

  public SpannerDataWriter(SpannerConfig spannerConfig) {
    this.spannerConfig = spannerConfig;
  }

  @Override
  public void write(List<Row> rows, DataGeneratorTable table) {
    if (spannerAccessor == null) {
      spannerAccessor = SpannerAccessor.getOrCreate(spannerConfig);
    }
    List<Mutation> mutations = new ArrayList<>();
    for (Row row : rows) {
      mutations.add(rowToMutation(table, row));
    }
    spannerAccessor.getDatabaseClient().writeAtLeastOnce(mutations);
  }

  private Mutation rowToMutation(DataGeneratorTable table, Row row) {
    Mutation.WriteBuilder builder = Mutation.newInsertOrUpdateBuilder(table.name());
    for (DataGeneratorColumn col : table.columns()) {
      Object val = getFieldFromRow(row, col.name());
      setColumnValueFromLogicalType(builder, col.name(), val, col.logicalType());
    }
    return builder.build();
  }

  private Object getFieldFromRow(Row row, String fieldName) {
    if (row.getSchema().hasField(fieldName)) {
      return row.getValue(fieldName);
    }
    return null; // Handle missing fields if any
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
              .to(
                  com.google.cloud.Timestamp.ofTimeMicroseconds(
                      ((Instant) value).getMillis() * 1000));
        } else {
          builder.set(columnName).to(value.toString());
        }
        break;
      default:
        builder.set(columnName).to(value.toString());
    }
  }

  @Override
  public void close() {
    if (spannerAccessor != null) {
      spannerAccessor.close();
    }
  }
}
