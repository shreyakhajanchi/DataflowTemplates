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
package com.custom;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.cloud.teleport.v2.spanner.ddl.Ddl;
import com.google.cloud.teleport.v2.spanner.migrations.cdc.TrimmedShardedDataChangeRecord;
import com.google.cloud.teleport.v2.spanner.migrations.schema.Schema;
import com.google.cloud.teleport.v2.spanner.migrations.utils.IShardIdFetcher;
import org.apache.beam.sdk.io.gcp.spanner.SpannerAccessor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Custom shard id function.
 */
public class CustomShardIdFetcher implements IShardIdFetcher {

    private static final Logger LOG = LoggerFactory.getLogger(CustomShardIdFetcher.class);

    public CustomShardIdFetcher() {
    }

    @Override
    public void init(
            SpannerAccessor spannerAccessor,
            Schema schema,
            Ddl ddl,
            ObjectMapper mapper,
            String shardName,
            String shardingMode) {
    }

    @Override
    public String getShardId(TrimmedShardedDataChangeRecord record) {
        LOG.info("Returning custom sharding function");
        return "xyz";
    }
}