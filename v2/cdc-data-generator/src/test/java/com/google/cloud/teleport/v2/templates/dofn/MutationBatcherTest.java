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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.anyInt;
import static org.mockito.Mockito.anyString;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

import com.google.cloud.teleport.v2.templates.model.DataGeneratorTable;
import com.google.cloud.teleport.v2.templates.sink.DataWriter;
import com.google.cloud.teleport.v2.templates.utils.Constants;
import com.google.common.collect.ImmutableList;
import java.util.Arrays;
import java.util.List;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.values.Row;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

@RunWith(JUnit4.class)
public class MutationBatcherTest {

  private DataWriter mockWriter;
  private MutationBatcher batcher;
  private DataGeneratorTable sampleTable;
  private Schema rowSchema;
  private Row sampleRow;
  private List<String> topoOrder;

  @Before
  public void setUp() {
    mockWriter = mock(DataWriter.class);
    batcher = new MutationBatcher(2, 10, mockWriter);
    batcher.startBundle();

    sampleTable =
        DataGeneratorTable.builder()
            .name("Users")
            .columns(ImmutableList.of())
            .primaryKeys(ImmutableList.of())
            .foreignKeys(ImmutableList.of())
            .uniqueKeys(ImmutableList.of())
            .insertQps(10)
            .isRoot(true)
            .build();

    rowSchema = Schema.builder().addInt64Field("id").build();
    sampleRow = Row.withSchema(rowSchema).addValue(1L).build();
    topoOrder = Arrays.asList("Users");
  }

  @Test
  public void testBufferRow_triggersFlushOnThreshold() {
    // Threshold is set to 2. Inserting 1st row should not trigger writer.
    batcher.bufferRow(
        "Users", sampleRow, Constants.MUTATION_INSERT, sampleTable, "shard0", topoOrder);
    verify(mockWriter, times(0)).insert(any(), any(), anyString(), anyInt());

    // Inserting 2nd row should cross threshold and flush inserts in topo order.
    batcher.bufferRow(
        "Users", sampleRow, Constants.MUTATION_INSERT, sampleTable, "shard0", topoOrder);
    verify(mockWriter, times(1)).insert(any(), any(), anyString(), anyInt());
  }

  @Test
  public void testBufferRow_sinkErrorRoutesToDlq() {
    doThrow(new RuntimeException("Sink error simulation"))
        .when(mockWriter)
        .insert(any(), any(), anyString(), anyInt());

    assertTrue(batcher.getPendingDlq().isEmpty());

    // Push two rows to force threshold trip and catch error routing
    batcher.bufferRow(
        "Users", sampleRow, Constants.MUTATION_INSERT, sampleTable, "shard0", topoOrder);
    batcher.bufferRow(
        "Users", sampleRow, Constants.MUTATION_INSERT, sampleTable, "shard0", topoOrder);

    assertFalse(batcher.getPendingDlq().isEmpty());
    assertEquals(2, batcher.getPendingDlq().size());
  }
}
