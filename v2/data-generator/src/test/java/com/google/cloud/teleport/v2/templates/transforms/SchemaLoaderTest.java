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
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertThrows;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.google.cloud.teleport.v2.templates.DataGeneratorOptions;
import com.google.cloud.teleport.v2.templates.DataGeneratorOptions.SinkType;
import com.google.cloud.teleport.v2.templates.common.SinkSchemaFetcher;
import com.google.cloud.teleport.v2.templates.model.DataGeneratorSchema;
import com.google.cloud.teleport.v2.templates.transforms.SchemaLoader.FetchSchemaFn;
import java.io.IOException;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.values.PCollectionView;
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
  public void testExpand() {
    pipeline.enableAbandonedNodeEnforcement(false);
    DataGeneratorOptions options = mock(DataGeneratorOptions.class);
    when(options.getSinkType()).thenReturn(SinkType.SPANNER);
    when(options.getSinkOptions()).thenReturn("options");

    SchemaLoader schemaLoader = new SchemaLoader(options);
    PCollectionView<DataGeneratorSchema> view = pipeline.apply(schemaLoader);

    assertNotNull(view);
  }

  @Test
  public void testFetchSchemaFn_Spanner() throws IOException {
    final SinkSchemaFetcher mockFetcher = mock(SinkSchemaFetcher.class);
    final DataGeneratorSchema mockSchema = mock(DataGeneratorSchema.class);
    when(mockFetcher.getSchema()).thenReturn(mockSchema);

    FetchSchemaFn fn =
        new FetchSchemaFn(SinkType.SPANNER, "options") {
          @Override
          protected SinkSchemaFetcher createFetcher(SinkType sinkType) {
            assertEquals(SinkType.SPANNER, sinkType);
            return mockFetcher;
          }

          @Override
          protected String readSinkOptions(String path) throws IOException {
            return "{}";
          }
        };

    DoFn.OutputReceiver<DataGeneratorSchema> receiver = mock(DoFn.OutputReceiver.class);
    fn.processElement(receiver);

    verify(mockFetcher).init("options", "{}");
    verify(receiver).output(mockSchema);
  }

  @Test
  public void testFetchSchemaFn_MySql() throws IOException {
    final SinkSchemaFetcher mockFetcher = mock(SinkSchemaFetcher.class);
    final DataGeneratorSchema mockSchema = mock(DataGeneratorSchema.class);
    when(mockFetcher.getSchema()).thenReturn(mockSchema);

    FetchSchemaFn fn =
        new FetchSchemaFn(SinkType.MYSQL, "options") {
          @Override
          protected SinkSchemaFetcher createFetcher(SinkType sinkType) {
            assertEquals(SinkType.MYSQL, sinkType);
            return mockFetcher;
          }

          @Override
          protected String readSinkOptions(String path) throws IOException {
            return "{}";
          }
        };

    DoFn.OutputReceiver<DataGeneratorSchema> receiver = mock(DoFn.OutputReceiver.class);
    fn.processElement(receiver);

    verify(mockFetcher).init("options", "{}");
    verify(receiver).output(mockSchema);
  }

  @Test
  public void testFetchSchemaFn_Unsupported() throws IOException {
    FetchSchemaFn fn = new FetchSchemaFn(null, "options") { // null or dummy enum if possible
          @Override
          protected String readSinkOptions(String path) throws IOException {
            return "{}";
          }
        };

    DoFn.OutputReceiver<DataGeneratorSchema> receiver = mock(DoFn.OutputReceiver.class);

    // unexpected IO exception for null sink type is not what we want to test, we
    // want to test createFetcher throwing
    // But wait, createFetcher is called inside processElement.
    // We didn't override createFetcher here so it uses the real one which throws
    // IllegalArgumentException
    // However, the real one checks for SPANNER and MYSQL. null will throw
    // "Unsupported sink type: null"

    assertThrows(IllegalArgumentException.class, () -> fn.processElement(receiver));
  }

  @Test
  public void testFetchSchemaFn_IOException() throws IOException {
    FetchSchemaFn fn =
        new FetchSchemaFn(SinkType.SPANNER, "options") {
          @Override
          protected String readSinkOptions(String path) throws IOException {
            throw new IOException("File not found");
          }
        };

    DoFn.OutputReceiver<DataGeneratorSchema> receiver = mock(DoFn.OutputReceiver.class);
    assertThrows(RuntimeException.class, () -> fn.processElement(receiver));
  }
}
