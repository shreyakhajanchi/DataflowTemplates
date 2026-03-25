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
    if (rows.isEmpty()) {
      return;
    }

    if (connectionHelper == null) {
      connectionHelper = new JdbcConnectionHelper();
    }

    // TODO: Resolve shard from the first row's _dg_shard_id and connect to it accurately
    // As a placeholder, assuming a single shard configuration or fetching connection here:
    // This will be expanded when Sharding routing is fully implemented.

    try {
      if (!connectionHelper.isConnectionPoolInitialized()) {
        ConnectionHelperRequest request = parseConnectionRequest(sinkOptionsJson);
        connectionHelper.init(request);
      }

      // Hardcode key for now until we parse it from the Shard.
      // The key format according to JdbcConnectionHelper is: sourceConnectionUrl + "/" + userName
      org.json.JSONObject json = new org.json.JSONObject(sinkOptionsJson);
      String jdbcUrl = json.optString("jdbcUrl", "");
      String user = json.optString("username", "");
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

  private ConnectionHelperRequest parseConnectionRequest(String sinkOptionsJson) {
    try {
      org.json.JSONObject json = new org.json.JSONObject(sinkOptionsJson);
      String jdbcUrl = json.optString("jdbcUrl", "");
      String user = json.optString("username", "");
      String password = json.optString("password", "");

      com.google.cloud.teleport.v2.spanner.migrations.shard.Shard shard =
          new com.google.cloud.teleport.v2.spanner.migrations.shard.Shard();
      shard.setLogicalShardId("shard0");
      shard.setUser(user);
      shard.setPassword(password);
      // JdbcConnectionHelper reconstructs jdbcUrl from Shard Host/Port/DB
      // For now we'll cheat or we need to parse jdbcUrl:
      // Assuming jdbc:mysql://host:port/db
      String cleanUrl = jdbcUrl.replace("jdbc:mysql://", "");
      String[] parts = cleanUrl.split("/");
      shard.setDbName(parts.length > 1 ? parts[1] : "");
      String[] hostPort = parts[0].split(":");
      shard.setHost(hostPort[0]);
      shard.setPort(hostPort.length > 1 ? hostPort[1] : "3306");

      // Default to 10 connections per pool for now. Can be configured later.
      return new ConnectionHelperRequest(
          java.util.Collections.singletonList(shard), "", 10, "com.mysql.cj.jdbc.Driver", "");
    } catch (Exception e) {
      LOG.error("Failed to parse sinkOptionsJson for MySQL connection: {}", sinkOptionsJson, e);
      throw new RuntimeException("Invalid sink configurations", e);
    }
  }

  @Override
  public void close() {
    // HikariCP connectionHelper is a singleton managed per worker. We do not explicitly close it
    // per writer.
  }
}
