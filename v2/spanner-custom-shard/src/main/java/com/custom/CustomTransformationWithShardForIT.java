/*
 * Copyright (C) 2024 Google LLC
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

import com.google.cloud.teleport.v2.spanner.utils.ISpannerMigrationTransformer;
import com.google.cloud.teleport.v2.spanner.utils.MigrationTransformationRequest;
import com.google.cloud.teleport.v2.spanner.utils.MigrationTransformationResponse;
import java.util.HashMap;
import java.util.Map;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class CustomTransformationWithShardForIT implements ISpannerMigrationTransformer {

  private static final Logger LOG = LoggerFactory.getLogger(CustomShardIdFetcher.class);

  @Override
  public void init(String parameters) {
    LOG.info("init called with {}", parameters);
  }

  @Override
  public MigrationTransformationResponse toSpannerRow(MigrationTransformationRequest request) {
    if (request.getTableName().equals("Customers")) {
      Map<String, Object> row = new HashMap<>(request.getRequestRow());
      row.put("full_name", row.get("first_name") + " " + row.get("last_name"));
      row.put("migration_shard_id", request.getShardId() + "_" + row.get("id"));
      MigrationTransformationResponse response = new MigrationTransformationResponse(row, false);
      return response;
    }
    return new MigrationTransformationResponse(request.getRequestRow(), false);
  }

  @Override
  public MigrationTransformationResponse toSourceRow(MigrationTransformationRequest request) {
    return null;
  }
}