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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThrows;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.google.cloud.spanner.Dialect;
import com.google.cloud.teleport.v2.templates.DataGeneratorOptions.SinkType;
import com.google.cloud.teleport.v2.templates.common.SinkSchemaFetcher;
import com.google.cloud.teleport.v2.templates.dofn.FetchSchemaFn;
import com.google.cloud.teleport.v2.templates.model.DataGeneratorSchema;
import com.google.cloud.teleport.v2.templates.model.MySqlSinkConfig;
import com.google.cloud.teleport.v2.templates.model.SpannerSinkConfig;
import com.google.common.collect.ImmutableMap;
import java.io.IOException;
import java.util.Collections;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.transforms.DoFn;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/** Unit tests for {@link SchemaLoader}. */
@RunWith(JUnit4.class)
public class SchemaLoaderTest {

  @Rule public final transient TestPipeline pipeline = TestPipeline.create();
  @Rule public ExpectedException expectedException = ExpectedException.none();

  @Test
  public void testFetchSchemaFn_Spanner() throws IOException {
    final SinkSchemaFetcher mockFetcher = mock(SinkSchemaFetcher.class);
    final DataGeneratorSchema schema =
        DataGeneratorSchema.builder().tables(ImmutableMap.of()).build();
    when(mockFetcher.getSchema()).thenReturn(schema);

    SpannerSinkConfig config = new SpannerSinkConfig("p", "i", "d", Dialect.GOOGLE_STANDARD_SQL);
    FetchSchemaFn fn =
        new FetchSchemaFn(SinkType.SPANNER, config) {
          @Override
          protected SinkSchemaFetcher createFetcher(SinkType sinkType) {
            assertEquals(SinkType.SPANNER, sinkType);
            return mockFetcher;
          }
        };

    DoFn.OutputReceiver<DataGeneratorSchema> receiver = mock(DoFn.OutputReceiver.class);
    fn.processElement(receiver);

    verify(mockFetcher).init(config);

    verify(receiver).output(schema);
  }

  @Test
  public void testFetchSchemaFn_MySql() throws IOException {
    final SinkSchemaFetcher mockFetcher = mock(SinkSchemaFetcher.class);
    final DataGeneratorSchema schema =
        DataGeneratorSchema.builder().tables(ImmutableMap.of()).build();
    when(mockFetcher.getSchema()).thenReturn(schema);

    MySqlSinkConfig config = new MySqlSinkConfig(Collections.emptyList());
    FetchSchemaFn fn =
        new FetchSchemaFn(SinkType.MYSQL, config) {
          @Override
          protected SinkSchemaFetcher createFetcher(SinkType sinkType) {
            assertEquals(SinkType.MYSQL, sinkType);
            return mockFetcher;
          }
        };

    DoFn.OutputReceiver<DataGeneratorSchema> receiver = mock(DoFn.OutputReceiver.class);
    fn.processElement(receiver);

    verify(mockFetcher).init(config);
    verify(receiver).output(schema);
  }

  @Test
  public void testFetchSchemaFn_Unsupported() throws IOException {
    SpannerSinkConfig config = new SpannerSinkConfig("p", "i", "d", Dialect.GOOGLE_STANDARD_SQL);
    FetchSchemaFn fn = new FetchSchemaFn(null, config);

    DoFn.OutputReceiver<DataGeneratorSchema> receiver = mock(DoFn.OutputReceiver.class);

    assertThrows(IllegalArgumentException.class, () -> fn.processElement(receiver));
  }

  @Test
  public void testFetchSchemaFn_IOException() throws IOException {
    final SinkSchemaFetcher mockFetcher = mock(SinkSchemaFetcher.class);
    when(mockFetcher.getSchema()).thenThrow(new IOException("File not found"));

    SpannerSinkConfig config = new SpannerSinkConfig("p", "i", "d", Dialect.GOOGLE_STANDARD_SQL);
    FetchSchemaFn fn =
        new FetchSchemaFn(SinkType.SPANNER, config) {
          @Override
          protected SinkSchemaFetcher createFetcher(SinkType sinkType) {
            return mockFetcher;
          }
        };

    DoFn.OutputReceiver<DataGeneratorSchema> receiver = mock(DoFn.OutputReceiver.class);
    assertThrows(RuntimeException.class, () -> fn.processElement(receiver));
  }
}
