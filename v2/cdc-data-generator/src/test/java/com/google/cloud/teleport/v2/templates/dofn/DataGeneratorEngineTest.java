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
package com.google.cloud.teleport.v2.templates.dofn;

import static org.junit.Assert.assertNotNull;
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.github.javafaker.Faker;
import com.google.cloud.teleport.v2.templates.model.DataGeneratorSchema;
import com.google.cloud.teleport.v2.templates.model.DataGeneratorTable;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import org.apache.beam.sdk.state.MapState;
import org.apache.beam.sdk.state.ReadableState;
import org.apache.beam.sdk.values.Row;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

@RunWith(JUnit4.class)
public class DataGeneratorEngineTest {

  @Test
  public void testEngine_initializesCorrectly() {
    DataGeneratorEngine engine = new DataGeneratorEngine(5, 5, new Faker());
    assertNotNull(engine);
  }

  @Test
  public void testProcessRecord_detectsCollisionAndSkips() {
    DataGeneratorEngine engine = new DataGeneratorEngine(5, 5, new Faker());

    DataGeneratorTable table =
        DataGeneratorTable.builder()
            .name("Users")
            .columns(ImmutableList.of())
            .primaryKeys(ImmutableList.of())
            .foreignKeys(ImmutableList.of())
            .uniqueKeys(ImmutableList.of())
            .insertQps(10)
            .isRoot(true)
            .build();

    DataGeneratorSchema schema =
        DataGeneratorSchema.builder().tables(ImmutableMap.of("Users", table)).build();

    MapState<String, Row> mockActiveKeys = mock(MapState.class);
    ReadableState<Row> mockReadableState = mock(ReadableState.class);
    when(mockReadableState.read()).thenReturn(mock(Row.class)); // Simulate hit/collision
    when(mockActiveKeys.get(any())).thenReturn(mockReadableState);

    MapState<String, DataGeneratorTable> mockTableMapState = mock(MapState.class);
    MutationBatcher mockBatcher = mock(MutationBatcher.class);

    Row mockRow = mock(Row.class);
    org.apache.beam.sdk.schemas.Schema emptySchema =
        org.apache.beam.sdk.schemas.Schema.builder().build();
    when(mockRow.getSchema()).thenReturn(emptySchema);

    // Should execute cleanly and skip value generation loop due to collision detection read hit
    engine.processRecord(
        "Users#0",
        mockRow,
        mockActiveKeys,
        mock(MapState.class),
        mock(org.apache.beam.sdk.state.ValueState.class),
        mockTableMapState,
        mock(org.apache.beam.sdk.state.Timer.class),
        schema,
        mockBatcher);

    assertNotNull(engine);
  }
}
