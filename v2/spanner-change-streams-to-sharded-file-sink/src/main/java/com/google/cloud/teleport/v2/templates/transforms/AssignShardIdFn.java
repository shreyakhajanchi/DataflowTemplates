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
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.cloud.teleport.v2.spanner.ddl.Ddl;
import com.google.cloud.teleport.v2.spanner.migrations.cdc.TrimmedShardedDataChangeRecord;
import com.google.cloud.teleport.v2.spanner.migrations.schema.Schema;
import com.google.cloud.teleport.v2.spanner.migrations.utils.IShardIdFetcher;
import com.google.cloud.teleport.v2.templates.utils.ShardIdFetcherImpl;
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
import java.util.List;
import org.apache.beam.sdk.io.FileSystems;
import org.apache.beam.sdk.io.fs.ResourceId;
import org.apache.beam.sdk.io.gcp.spanner.SpannerAccessor;
import org.apache.beam.sdk.io.gcp.spanner.SpannerConfig;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.DoFn.ProcessContext;
import org.apache.beam.sdk.transforms.DoFn.ProcessElement;
import org.apache.beam.sdk.util.MimeTypes;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.base.Splitter;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.io.ByteStreams;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.io.Files;
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

  private final String customJarPath;

  private final String shardingCustomClassName;

  private IShardIdFetcher shardIdFetcher;

  public AssignShardIdFn(
          SpannerConfig spannerConfig,
          Schema schema,
          Ddl ddl,
          String shardingMode,
          String shardName,
          String customJarPath,
          String shardingCustomClassName) {
    this.spannerConfig = spannerConfig;
    this.schema = schema;
    this.ddl = ddl;
    this.shardingMode = shardingMode;
    this.shardName = shardName;
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
    shardIdFetcher =
            getShardIdFetcherImpl(
                    spannerAccessor,
                    schema,
                    shardName,
                    ddl,
                    mapper,
                    shardingMode,
                    customJarPath,
                    shardingCustomClassName);
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
      String shardId = shardIdFetcher.getShardId(record);
      record.setShard(shardId);
      c.output(record);
    } catch (Exception e) {
      StringWriter errors = new StringWriter();
      e.printStackTrace(new PrintWriter(errors));
      LOG.error("Error fetching shard Id colum: " + e.getMessage() + ": " + errors.toString());
      return;
    }
  }

  private IShardIdFetcher getShardIdFetcherImpl(
          SpannerAccessor spannerAccessor,
          Schema schema,
          String shardName,
          Ddl ddl,
          ObjectMapper mapper,
          String shardingMode,
          String customJarPath,
          String shardingCustomClassName) {
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

        shardFetcher.init(spannerAccessor, schema, ddl, mapper, shardName, shardingMode);
        return shardFetcher;
      } catch (Exception e) {
        throw new RuntimeException("Error loading custom class : " + e.getMessage());
      }
    }
    // else return the core implementation
    ShardIdFetcherImpl shardIdFetcher = new ShardIdFetcherImpl();
    shardIdFetcher.init(spannerAccessor, schema, ddl, mapper, shardName, shardingMode);
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
}