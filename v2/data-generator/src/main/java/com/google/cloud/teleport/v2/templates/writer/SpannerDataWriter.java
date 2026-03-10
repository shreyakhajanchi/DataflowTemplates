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

import com.google.cloud.spanner.Mutation;
import com.google.cloud.teleport.v2.templates.DataGeneratorOptions;
import com.google.cloud.teleport.v2.templates.model.DataGeneratorTable;
import java.util.List;
import org.apache.beam.sdk.io.gcp.spanner.SpannerAccessor;
import org.apache.beam.sdk.io.gcp.spanner.SpannerConfig;
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
  public void write(List<Mutation> mutations, DataGeneratorTable table) {
    if (spannerAccessor == null) {
      spannerAccessor = SpannerAccessor.getOrCreate(spannerConfig);
    }
    spannerAccessor.getDatabaseClient().writeAtLeastOnce(mutations);
  }

  @Override
  public void close() {
    if (spannerAccessor != null) {
      spannerAccessor.close();
    }
  }
}
