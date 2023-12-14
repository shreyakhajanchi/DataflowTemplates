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
package com.google.cloud.teleport.v2.templates.utils;

import com.google.cloud.teleport.v2.spanner.migrations.utils.IShardIdFetcher;
import java.io.PrintWriter;
import java.io.StringWriter;
import org.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Core implemenation to get the shard identifier. */
public class ShardIdFetcherImpl implements IShardIdFetcher {
  private static final Logger LOG = LoggerFactory.getLogger(ShardIdFetcherImpl.class);

  public ShardIdFetcherImpl() {}

  @Override
  public String getShardId(JSONObject record) {
    try {
      String shardIdColumn = record.getString("shardIdColumn");
      if (record.getJSONObject("dataRecord").has(shardIdColumn)) {
        String shardId = record.getJSONObject("dataRecord").get(shardIdColumn).toString();
        return shardId;
      } else {
        LOG.error(
            "Cannot find entry for the shard id column '"
                + record.get("shardIdColumn").toString()
                + "' in record.");
        throw new RuntimeException(
            "Cannot find entry for the shard id column '"
                + record.get("shardIdColumn").toString()
                + "' in record.");
      }

      /*if (keysJson.has(shardIdColumn)) {
        String shardId = keysJson.get(shardIdColumn).asText();
        return shardId;
      } else if (newValueJson.has(shardIdColumn)) {
        String shardId = newValueJson.get(shardIdColumn).asText();
        return shardId;
      } else if (record.getModType() == ModType.DELETE) {
        String shardId =
            fetchShardId(
                record.getTableName(), record.getCommitTimestamp(), shardIdColumn, keysJson);
        return shardId;
      }*/
    } catch (IllegalArgumentException e) {
      LOG.error("Error fetching shard Id column for table: " + e.getMessage());
      throw new RuntimeException("Error fetching shard Id column for table: " + e.getMessage());
    } catch (Exception e) {
      StringWriter errors = new StringWriter();
      e.printStackTrace(new PrintWriter(errors));
      LOG.error("Error fetching shard Id colum: " + e.getMessage() + ": " + errors.toString());
      throw new RuntimeException(
          "Error fetching shard Id colum: " + e.getMessage() + ": " + errors.toString());
    }
  }
}
