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
package com.google.cloud.teleport.v2.spanner.migrations.utils;

/** The interface to get the shard identifier. */
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.cloud.teleport.v2.spanner.ddl.Ddl;
import com.google.cloud.teleport.v2.spanner.migrations.cdc.TrimmedShardedDataChangeRecord;
import com.google.cloud.teleport.v2.spanner.migrations.schema.Schema;
import org.apache.beam.sdk.io.gcp.spanner.SpannerAccessor;

public interface IShardIdFetcher {

  void init(
      SpannerAccessor spannerAccessor,
      Schema schema,
      Ddl ddl,
      ObjectMapper mapper,
      String shardName,
      String shardingMode);

  String getShardId(TrimmedShardedDataChangeRecord record);
}
