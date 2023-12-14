/*
 * Copyright (C) 2023 Google LLC
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

import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.cloud.spanner.Struct;
import com.google.cloud.spanner.TimestampBound;
import com.google.cloud.teleport.v2.spanner.ddl.Column;
import com.google.cloud.teleport.v2.spanner.ddl.Ddl;
import com.google.cloud.teleport.v2.spanner.ddl.IndexColumn;
import com.google.cloud.teleport.v2.spanner.ddl.Table;
import com.google.cloud.teleport.v2.spanner.migrations.cdc.TrimmedShardedDataChangeRecord;
import com.google.cloud.teleport.v2.spanner.migrations.schema.Schema;
import com.google.cloud.teleport.v2.spanner.migrations.schema.SpannerTable;
import com.google.cloud.teleport.v2.spanner.migrations.utils.IShardIdFetcher;
import com.google.cloud.teleport.v2.spanner.type.Type;
import com.google.cloud.teleport.v2.templates.changestream.DataChangeRecordTypeConvertor;
import com.google.cloud.teleport.v2.templates.constants.Constants;
import com.google.cloud.teleport.v2.templates.utils.ShardIdFetcherImpl;
import com.google.common.collect.ImmutableList;
import java.io.File;
import java.io.IOException;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.lang.reflect.Constructor;
import java.net.URL;
import java.net.URLClassLoader;
import java.nio.channels.ReadableByteChannel;
import java.nio.channels.WritableByteChannel;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.stream.Collectors;
import org.apache.beam.sdk.io.FileSystems;
import org.apache.beam.sdk.io.fs.ResourceId;
import org.apache.beam.sdk.io.gcp.spanner.SpannerAccessor;
import org.apache.beam.sdk.io.gcp.spanner.SpannerConfig;
import org.apache.beam.sdk.io.gcp.spanner.changestreams.model.ModType;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.util.MimeTypes;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.base.Splitter;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.io.ByteStreams;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.io.Files;
import org.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** This DoFn assigns the shardId as key to the record. */
public class AssignShardIdFn
    extends DoFn<TrimmedShardedDataChangeRecord, TrimmedShardedDataChangeRecord> {
  private static final Logger LOG = LoggerFactory.getLogger(AssignShardIdFn.class);

  private final SpannerConfig spannerConfig;

  /* SpannerAccessor must be transient so that its value is not serialized at runtime. */
  private transient SpannerAccessor spannerAccessor;

  /* The information schema of the Cloud Spanner database */
  private final Ddl ddl;

  private final Schema schema;

  // Jackson Object mapper.
  private transient ObjectMapper mapper;

  private final String shardingMode;

  private final String shardName;

  private final String skipDirName;

  private final String customJarPath;

  private final String shardingCustomClassName;

  private IShardIdFetcher shardIdFetcher;

  public AssignShardIdFn(
      SpannerConfig spannerConfig,
      Schema schema,
      Ddl ddl,
      String shardingMode,
      String shardName,
      String skipDirName,
      String customJarPath,
      String shardingCustomClassName) {
    this.spannerConfig = spannerConfig;
    this.schema = schema;
    this.ddl = ddl;
    this.shardingMode = shardingMode;
    this.shardName = shardName;
    this.skipDirName = skipDirName;
    this.customJarPath = customJarPath;
    this.shardingCustomClassName = shardingCustomClassName;
  }

  /** Setup function connects to Cloud Spanner. */
  @Setup
  public void setup() {
    if (spannerConfig != null) {
      spannerAccessor = SpannerAccessor.getOrCreate(spannerConfig);
    }
    mapper = new ObjectMapper();
    mapper.enable(DeserializationFeature.USE_BIG_DECIMAL_FOR_FLOATS);
    shardIdFetcher = getShardIdFetcherImpl(customJarPath, shardingCustomClassName);
  }

  /** Teardown function disconnects from the Cloud Spanner. */
  @Teardown
  public void teardown() {
    if (spannerConfig != null) {
      spannerAccessor.close();
    }
  }

  @ProcessElement
  public void processElement(ProcessContext c) {
    TrimmedShardedDataChangeRecord record = new TrimmedShardedDataChangeRecord(c.element());

    try {
      if (shardingMode.equals(Constants.SHARDING_MODE_SINGLE_SHARD)) {
        record.setShard(this.shardName);
        c.output(record);
      } else {
        String shardIdColumn = getShardIdColumnForTableName(record.getTableName());
        JSONObject object = new JSONObject();
        object.put("shardIdColumn", shardIdColumn);
        String keysJsonStr = record.getMods().get(0).getKeysJson();
        JsonNode keysJson = mapper.readTree(keysJsonStr);

        String newValueJsonStr = record.getMods().get(0).getNewValuesJson();
        JsonNode newValueJson = mapper.readTree(newValueJsonStr);
        JSONObject dataRecord;
        if (record.getModType() == ModType.DELETE) {
          dataRecord = fetchShardId(record.getTableName(), record.getCommitTimestamp(), keysJson);
        } else {
          dataRecord = new JSONObject();
          // Add all fields from keysJson to dataRecord
          for (Iterator<String> it = keysJson.fieldNames(); it.hasNext(); ) {
            String key = it.next();
            dataRecord.put(key, keysJson.get(key));
          }
          for (Iterator<String> it = newValueJson.fieldNames(); it.hasNext(); ) {
            String key = it.next();
            dataRecord.put(key, newValueJson.get(key));
          }
        }
        object.put("dataRecord", dataRecord);

        String shardId = shardIdFetcher.getShardId(object);
        record.setShard(shardId);
        c.output(record);
      }

    } catch (IllegalArgumentException e) {
      LOG.error("Error fetching shard Id column for table: " + e.getMessage());
    } catch (Exception e) {
      StringWriter errors = new StringWriter();
      e.printStackTrace(new PrintWriter(errors));
      LOG.error("Error fetching shard Id colum: " + e.getMessage() + ": " + errors.toString());
    }
  }

  private IShardIdFetcher getShardIdFetcherImpl(
      String customJarPath, String shardingCustomClassName) {
    if (!customJarPath.isEmpty() && !shardingCustomClassName.isEmpty()) {
      LOG.info(
          "Getting custom sharding fetcher : "
              + customJarPath
              + " with class: "
              + shardingCustomClassName);
      try {

        // Getting the jar URL which contains target class
        URL[] classLoaderUrls = saveFilesLocally(customJarPath);

        // Create a new URLClassLoader
        URLClassLoader urlClassLoader = new URLClassLoader(classLoaderUrls);

        // Load the target class
        Class<?> shardFetcherClass = urlClassLoader.loadClass(shardingCustomClassName);

        // Create a new instance from the loaded class
        Constructor<?> constructor = shardFetcherClass.getConstructor();
        IShardIdFetcher shardFetcher = (IShardIdFetcher) constructor.newInstance();
        return shardFetcher;
      } catch (Exception e) {
        throw new RuntimeException("Error loading custom class : " + e.getMessage());
      }
    }
    // else return the core implementation
    ShardIdFetcherImpl shardIdFetcher = new ShardIdFetcherImpl();
    return shardIdFetcher;
  }

  private URL[] saveFilesLocally(String driverJars) {
    List<String> listOfJarPaths = Splitter.on(',').trimResults().splitToList(driverJars);

    final String destRoot = Files.createTempDir().getAbsolutePath();
    List<URL> driverJarUrls = new ArrayList<>();
    listOfJarPaths.stream()
        .forEach(
            jarPath -> {
              try {
                ResourceId sourceResourceId = FileSystems.matchNewResource(jarPath, false);
                @SuppressWarnings("nullness")
                File destFile = Paths.get(destRoot, sourceResourceId.getFilename()).toFile();
                ResourceId destResourceId =
                    FileSystems.matchNewResource(destFile.getAbsolutePath(), false);
                copy(sourceResourceId, destResourceId);
                LOG.info("Localized jar: " + sourceResourceId + " to: " + destResourceId);
                driverJarUrls.add(destFile.toURI().toURL());
              } catch (IOException e) {
                LOG.warn("Unable to copy " + jarPath, e);
              }
            });
    return driverJarUrls.stream().toArray(URL[]::new);
  }

  private void copy(ResourceId source, ResourceId dest) throws IOException {
    try (ReadableByteChannel rbc = FileSystems.open(source)) {
      try (WritableByteChannel wbc = FileSystems.create(dest, MimeTypes.BINARY)) {
        ByteStreams.copy(rbc, wbc);
      }
    }
  }

  private String getShardIdColumnForTableName(String tableName) throws IllegalArgumentException {
    if (!schema.getSpannerToID().containsKey(tableName)) {
      throw new IllegalArgumentException(
          "Table " + tableName + " found in change record but not found in session file.");
    }
    String tableId = schema.getSpannerToID().get(tableName).getName();
    if (!schema.getSpSchema().containsKey(tableId)) {
      throw new IllegalArgumentException(
          "Table " + tableId + " not found in session file. Please provide a valid session file.");
    }
    SpannerTable spTable = schema.getSpSchema().get(tableId);
    String shardColId = spTable.getShardIdColumn();
    if (!spTable.getColDefs().containsKey(shardColId)) {
      throw new IllegalArgumentException(
          "ColumnId "
              + shardColId
              + " not found in session file. Please provide a valid session file.");
    }
    return spTable.getColDefs().get(shardColId).getName();
  }

  private JSONObject fetchShardId(
      String tableName, com.google.cloud.Timestamp commitTimestamp, JsonNode keysJson)
      throws Exception {
    com.google.cloud.Timestamp staleReadTs =
        com.google.cloud.Timestamp.ofTimeSecondsAndNanos(
            commitTimestamp.getSeconds() - 1, commitTimestamp.getNanos());
    List<String> columns =
        ddl.table(tableName).columns().stream().map(Column::name).collect(Collectors.toList());
    Struct row =
        spannerAccessor
            .getDatabaseClient()
            .singleUse(TimestampBound.ofReadTimestamp(staleReadTs))
            .readRow(tableName, generateKey(tableName, keysJson), columns);
    if (row == null) {
      throw new Exception("stale read on Spanner returned null");
    }
    return getRowAsJSONObject(row, columns);
  }

  private static JSONObject getRowAsJSONObject(Struct row, List<String> columns) {
    JSONObject jsonObject = new JSONObject();

    for (String columnName : columns) {
      Object columnValue = row.isNull(columnName) ? null : row.getValue(columnName);
      jsonObject.put(columnName, columnValue);
    }
    return jsonObject;
  }

  private com.google.cloud.spanner.Key generateKey(String tableName, JsonNode keysJson)
      throws Exception {
    try {
      Table table = ddl.table(tableName);
      ImmutableList<IndexColumn> keyColumns = table.primaryKeys();
      com.google.cloud.spanner.Key.Builder pk = com.google.cloud.spanner.Key.newBuilder();

      for (IndexColumn keyColumn : keyColumns) {
        Column key = table.column(keyColumn.name());
        Type keyColType = key.type();
        String keyColName = key.name();
        switch (keyColType.getCode()) {
          case BOOL:
          case PG_BOOL:
            pk.append(
                DataChangeRecordTypeConvertor.toBoolean(
                    keysJson, keyColName, /* requiredField= */ true));
            break;
          case INT64:
          case PG_INT8:
            pk.append(
                DataChangeRecordTypeConvertor.toLong(
                    keysJson, keyColName, /* requiredField= */ true));
            break;
          case FLOAT64:
          case PG_FLOAT8:
            pk.append(
                DataChangeRecordTypeConvertor.toDouble(
                    keysJson, keyColName, /* requiredField= */ true));
            break;
          case STRING:
          case PG_VARCHAR:
          case PG_TEXT:
            pk.append(
                DataChangeRecordTypeConvertor.toString(
                    keysJson, keyColName, /* requiredField= */ true));
            break;
          case NUMERIC:
          case PG_NUMERIC:
            pk.append(
                DataChangeRecordTypeConvertor.toNumericBigDecimal(
                    keysJson, keyColName, /* requiredField= */ true));
            break;
          case JSON:
          case PG_JSONB:
            pk.append(
                DataChangeRecordTypeConvertor.toString(
                    keysJson, keyColName, /* requiredField= */ true));
            break;
          case BYTES:
          case PG_BYTEA:
            pk.append(
                DataChangeRecordTypeConvertor.toByteArray(
                    keysJson, keyColName, /* requiredField= */ true));
            break;
          case TIMESTAMP:
          case PG_TIMESTAMPTZ:
            pk.append(
                DataChangeRecordTypeConvertor.toTimestamp(
                    keysJson, keyColName, /* requiredField= */ true));
            break;
          case DATE:
          case PG_DATE:
            pk.append(
                DataChangeRecordTypeConvertor.toDate(
                    keysJson, keyColName, /* requiredField= */ true));
            break;
          default:
            throw new IllegalArgumentException(
                "Column name(" + keyColName + ") has unsupported column type(" + keyColType + ")");
        }
      }
      return pk.build();
    } catch (Exception e) {
      throw new Exception("Error generating key: " + e.getMessage());
    }
  }
}
