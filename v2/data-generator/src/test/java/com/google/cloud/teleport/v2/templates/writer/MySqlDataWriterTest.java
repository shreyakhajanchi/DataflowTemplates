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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

import com.google.cloud.teleport.v2.spanner.migrations.connection.ConnectionHelperRequest;
import com.google.cloud.teleport.v2.spanner.migrations.shard.Shard;
import org.junit.Test;

public class MySqlDataWriterTest {

  @Test
  public void testParseConnectionRequest() {
    String shardJson =
        "[{\"logicalShardId\": \"shard1\", \"host\": \"localhost\", \"port\": \"3306\", \"user\": \"root\", \"password\": \"pass\", \"dbName\": \"db\", \"connectionProperties\": \"useSSL=false\"}]";
    MySqlDataWriter writer = new MySqlDataWriter(shardJson);
    ConnectionHelperRequest request = writer.parseConnectionRequest(shardJson);

    assertNotNull(request);
    assertEquals(1, request.getShards().size());
    Shard shard = request.getShards().get(0);
    assertEquals("root", shard.getUserName());
    assertEquals("pass", shard.getPassword());
    assertEquals("localhost", shard.getHost());
    assertEquals("3306", shard.getPort());
    assertEquals("db", shard.getDbName());
    assertEquals("useSSL=false", shard.getConnectionProperties());
  }

  @Test(expected = RuntimeException.class)
  public void testParseConnectionRequestEmptyArray() {
    String shardJson = "[]";
    MySqlDataWriter writer = new MySqlDataWriter(shardJson);
    writer.parseConnectionRequest(shardJson);
  }

  @Test(expected = RuntimeException.class)
  public void testParseConnectionRequestInvalidJson() {
    String shardJson = "not a json";
    MySqlDataWriter writer = new MySqlDataWriter(shardJson);
    writer.parseConnectionRequest(shardJson);
  }

  @Test
  public void testParseConnectionRequestMultipleShards() {
    String shardJson =
        "["
            + "{\"logicalShardId\": \"shard1\", \"host\": \"host1\", \"port\": \"3306\", \"user\": \"user1\", \"password\": \"pass1\", \"dbName\": \"db1\"},"
            + "{\"logicalShardId\": \"shard2\", \"host\": \"host2\", \"port\": \"3306\", \"user\": \"user2\", \"password\": \"pass2\", \"dbName\": \"db2\"}"
            + "]";
    MySqlDataWriter writer = new MySqlDataWriter(shardJson);
    ConnectionHelperRequest request = writer.parseConnectionRequest(shardJson);

    assertNotNull(request);
    assertEquals(2, request.getShards().size());

    Shard shard1 = request.getShards().get(0);
    assertEquals("shard1", shard1.getLogicalShardId());
    assertEquals("user1", shard1.getUserName());

    Shard shard2 = request.getShards().get(1);
    assertEquals("shard2", shard2.getLogicalShardId());
    assertEquals("user2", shard2.getUserName());
  }
}
