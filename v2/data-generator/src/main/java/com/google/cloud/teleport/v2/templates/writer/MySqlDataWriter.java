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

import com.google.cloud.teleport.v2.spanner.migrations.connection.ConnectionHelperRequest;
import com.google.cloud.teleport.v2.spanner.migrations.connection.JdbcConnectionHelper;
import com.google.cloud.teleport.v2.spanner.migrations.exceptions.ConnectionException;
import com.google.cloud.teleport.v2.templates.model.DataGeneratorColumn;
import com.google.cloud.teleport.v2.templates.model.DataGeneratorTable;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.List;
import org.apache.beam.sdk.values.Row;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** MySQL implementation of {@link DataWriter}. */
public class MySqlDataWriter implements DataWriter {
  private static final Logger LOG = LoggerFactory.getLogger(MySqlDataWriter.class);
  private final String sinkOptionsJson;
  private transient JdbcConnectionHelper connectionHelper;

  public MySqlDataWriter(String sinkOptionsJson) {
    this.sinkOptionsJson = sinkOptionsJson;
  }

  @Override
  public void write(List<Row> rows, DataGeneratorTable table) {
    write(rows, table, "");
  }

  public void write(List<Row> rows, DataGeneratorTable table, String shardId) {
    if (rows.isEmpty()) {
      return;
    }
    if (connectionHelper == null) {
      connectionHelper =
          new com.google.cloud.teleport.v2.spanner.migrations.connection.JdbcConnectionHelper();
    }
    try {
      if (!connectionHelper.isConnectionPoolInitialized()) {
        ConnectionHelperRequest request = parseConnectionRequest(sinkOptionsJson);
        connectionHelper.init(request);
      }

      org.json.JSONArray jsonArray = new org.json.JSONArray(sinkOptionsJson);
      org.json.JSONObject targetShardJson = null;

      if (!shardId.isEmpty()) {
        for (int i = 0; i < jsonArray.length(); i++) {
          org.json.JSONObject shardJson = jsonArray.getJSONObject(i);
          if (shardId.equals(shardJson.optString("logicalShardId", ""))) {
            targetShardJson = shardJson;
            break;
          }
        }
      }

      if (targetShardJson == null) {
        // Fallback to first shard if not found or shardId is empty
        targetShardJson = jsonArray.getJSONObject(0);
      }

      String user = targetShardJson.optString("user", "");
      String host = targetShardJson.optString("host", "");
      String port = targetShardJson.optString("port", "3306");
      String dbName = targetShardJson.optString("dbName", "");
      String jdbcUrl = String.format("jdbc:mysql://%s:%s/%s", host, port, dbName);
      String connectionKey = jdbcUrl + "/" + user;

      try (Connection connection = connectionHelper.getConnection(connectionKey)) {
        writeRowsToConnection(connection, rows, table);
      }
    } catch (ConnectionException | SQLException e) {
      LOG.error("Failed to write to MySQL", e);
      throw new RuntimeException("Failed to write to MySQL", e);
    }
  }

  private void writeRowsToConnection(
      Connection connection, List<Row> rows, DataGeneratorTable table) throws SQLException {
    if (rows.isEmpty()) {
      return;
    }
    String sql = buildInsertSql(table);
    try (PreparedStatement statement = connection.prepareStatement(sql)) {
      for (Row row : rows) {
        setStatementParameters(statement, row, table);
        statement.addBatch();
      }
      statement.executeBatch();
    }
  }

  private String buildInsertSql(DataGeneratorTable table) {
    StringBuilder sql = new StringBuilder("INSERT INTO ");
    sql.append(table.name()).append(" (");

    List<DataGeneratorColumn> columns = table.columns();
    for (int i = 0; i < columns.size(); i++) {
      sql.append(columns.get(i).name());
      if (i < columns.size() - 1) {
        sql.append(", ");
      }
    }
    sql.append(") VALUES (");

    for (int i = 0; i < columns.size(); i++) {
      sql.append("?");
      if (i < columns.size() - 1) {
        sql.append(", ");
      }
    }
    sql.append(")");
    return sql.toString();
  }

  private void setStatementParameters(
      PreparedStatement statement, Row row, DataGeneratorTable table) throws SQLException {
    List<DataGeneratorColumn> columns = table.columns();
    for (int i = 0; i < columns.size(); i++) {
      DataGeneratorColumn col = columns.get(i);
      Object val = null;
      if (row.getSchema().hasField(col.name())) {
        val = row.getValue(col.name());
      }
      statement.setObject(i + 1, val);
    }
  }

  ConnectionHelperRequest parseConnectionRequest(String sinkOptionsJson) {
    try {
      org.json.JSONArray jsonArray = new org.json.JSONArray(sinkOptionsJson);
      if (jsonArray.length() == 0) {
        throw new RuntimeException("No shards found in sink options");
      }

      List<com.google.cloud.teleport.v2.spanner.migrations.shard.Shard> shards =
          new java.util.ArrayList<>();

      for (int i = 0; i < jsonArray.length(); i++) {
        org.json.JSONObject shardJson = jsonArray.getJSONObject(i);
        String user = shardJson.optString("user", "");
        String password = shardJson.optString("password", "");
        String host = shardJson.optString("host", "");
        String port = shardJson.optString("port", "3306");
        String dbName = shardJson.optString("dbName", "");
        String connectionProperties = shardJson.optString("connectionProperties", "");

        com.google.cloud.teleport.v2.spanner.migrations.shard.Shard shard =
            new com.google.cloud.teleport.v2.spanner.migrations.shard.Shard();
        shard.setLogicalShardId(shardJson.optString("logicalShardId", "shard" + i));
        shard.setUser(user);
        shard.setPassword(password);
        shard.setHost(host);
        shard.setPort(port);
        shard.setDbName(dbName);
        shard.setConnectionProperties(connectionProperties);
        shards.add(shard);
      }

      // Default to 10 connections per pool for now. Can be configured later.
      return new ConnectionHelperRequest(shards, "", 10, "com.mysql.cj.jdbc.Driver", "");
    } catch (Exception e) {
      throw new RuntimeException("Failed to parse connection request", e);
    }
  }

  @Override
  public void close() {
    // HikariCP connectionHelper is a singleton managed per worker. We do not explicitly close it
    // per writer.
  }
}
