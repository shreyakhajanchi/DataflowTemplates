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
package com.google.cloud.teleport.v2.templates.transforms;

import com.google.cloud.teleport.v2.templates.DataGeneratorOptions;
import com.google.cloud.teleport.v2.templates.DataGeneratorOptions.SinkType;
import com.google.cloud.teleport.v2.templates.common.SinkSchemaFetcher;
import com.google.cloud.teleport.v2.templates.model.DataGeneratorSchema;
import com.google.cloud.teleport.v2.templates.mysql.MySqlSchemaFetcher;
import com.google.cloud.teleport.v2.templates.spanner.SpannerSchemaFetcher;
import com.google.common.io.CharStreams;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.Reader;
import java.nio.channels.Channels;
import java.nio.channels.ReadableByteChannel;
import java.nio.charset.StandardCharsets;
import org.apache.beam.sdk.io.FileSystems;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.View;
import org.apache.beam.sdk.values.PBegin;
import org.apache.beam.sdk.values.PCollectionView;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A {@link PTransform} that loads the {@link DataGeneratorSchema} from the sink as a side input.
 */
public class SchemaLoader extends PTransform<PBegin, PCollectionView<DataGeneratorSchema>> {

  private static final Logger LOG = LoggerFactory.getLogger(SchemaLoader.class);
  private final DataGeneratorOptions options;

  public SchemaLoader(DataGeneratorOptions options) {
    this.options = options;
  }

  @Override
  public PCollectionView<DataGeneratorSchema> expand(PBegin input) {
    return input
        .apply("CreateSinkType", Create.of(options.getSinkType()))
        .apply(
            "FetchSchema",
            ParDo.of(new FetchSchemaFn(options.getSinkType(), options.getSinkOptions())))
        .apply("ViewAsSingleton", View.asSingleton());
  }

  static class FetchSchemaFn extends DoFn<SinkType, DataGeneratorSchema> {
    private final SinkType sinkType;
    private final String sinkOptionsPath;

    FetchSchemaFn(SinkType sinkType, String sinkOptionsPath) {
      this.sinkType = sinkType;
      this.sinkOptionsPath = sinkOptionsPath;
    }

    @ProcessElement
    public void processElement(OutputReceiver<DataGeneratorSchema> receiver) {
      try {
        String sinkOptionsJson = readSinkOptions(sinkOptionsPath);
        SinkSchemaFetcher fetcher = createFetcher(sinkType);

        fetcher.init(sinkOptionsPath, sinkOptionsJson);
        DataGeneratorSchema schema = fetcher.getSchema();
        LOG.info("Fetched Schema: {}", schema);
        receiver.output(schema);
      } catch (IOException e) {
        throw new RuntimeException("Failed to fetch schema", e);
      }
    }

    /**
     * Creates a {@link SinkSchemaFetcher} based on the sink type. This method is protected to allow
     * overriding in tests.
     *
     * @param sinkType The sink type.
     * @return The {@link SinkSchemaFetcher}.
     */
    protected SinkSchemaFetcher createFetcher(SinkType sinkType) {
      if (sinkType == SinkType.SPANNER) {
        return new SpannerSchemaFetcher();
      } else if (sinkType == SinkType.MYSQL) {
        return new MySqlSchemaFetcher();
      } else {
        throw new IllegalArgumentException("Unsupported sink type: " + sinkType);
      }
    }

    protected String readSinkOptions(String path) throws IOException {
      try (ReadableByteChannel channel =
          FileSystems.open(FileSystems.matchNewResource(path, false))) {
        try (Reader reader =
            new InputStreamReader(Channels.newInputStream(channel), StandardCharsets.UTF_8)) {
          return CharStreams.toString(reader);
        }
      }
    }
  }
}
