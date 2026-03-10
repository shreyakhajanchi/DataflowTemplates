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
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;

import com.google.cloud.teleport.v2.templates.model.DataGeneratorSchema;
import com.google.cloud.teleport.v2.templates.model.DataGeneratorTable;
import com.google.cloud.teleport.v2.templates.transforms.SelectTable.SelectTableFn;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.util.NavigableMap;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.values.PCollectionView;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/** Tests for {@link SelectTable}. */
@RunWith(JUnit4.class)
public class SelectTableTest {

  @Rule public final transient TestPipeline pipeline = TestPipeline.create();

  @Test
  public void testWeightedSelectionLogic() throws Exception {
    // Schema:
    // Root A (10 QPS) has Child B (100 QPS)
    // Root C (50 QPS)
    // Total QPS = 160.
    // Weight A = 110 (10+100). Prob = 110/160 = 0.6875
    // Weight C = 50. Prob = 50/160 = 0.3125

    DataGeneratorTable tableA =
        DataGeneratorTable.builder()
            .name("A")
            .qps(10)
            .isRoot(true)
            .columns(ImmutableList.of())
            .primaryKeys(ImmutableList.of())
            .foreignKeys(ImmutableList.of())
            .uniqueKeys(ImmutableList.of())
            .build();

    DataGeneratorTable tableB =
        DataGeneratorTable.builder()
            .name("B")
            .qps(100)
            .isRoot(false)
            .interleavedInTable("A")
            .columns(ImmutableList.of())
            .primaryKeys(ImmutableList.of())
            .foreignKeys(ImmutableList.of())
            .uniqueKeys(ImmutableList.of())
            .build();

    DataGeneratorTable tableC =
        DataGeneratorTable.builder()
            .name("C")
            .qps(50)
            .isRoot(true)
            .columns(ImmutableList.of())
            .primaryKeys(ImmutableList.of())
            .foreignKeys(ImmutableList.of())
            .uniqueKeys(ImmutableList.of())
            .build();

    DataGeneratorSchema schema =
        DataGeneratorSchema.builder()
            .tables(ImmutableMap.of("A", tableA, "B", tableB, "C", tableC))
            .dialect(com.google.cloud.teleport.v2.templates.model.SinkDialect.GOOGLE_STANDARD_SQL)
            .build();

    PCollectionView<DataGeneratorSchema> schemaView = mock(PCollectionView.class);
    SelectTableFn fn = new SelectTableFn(schemaView);

    // Use reflection to invoke initializeSelectionMap
    Method initMethod =
        SelectTableFn.class.getDeclaredMethod("initializeSelectionMap", DataGeneratorSchema.class);
    initMethod.setAccessible(true);
    initMethod.invoke(fn, schema);

    // Inspect the map
    Field mapField = SelectTableFn.class.getDeclaredField("tableSelectionMap");
    mapField.setAccessible(true);
    NavigableMap<Double, DataGeneratorTable> map =
        (NavigableMap<Double, DataGeneratorTable>) mapField.get(fn);

    assertEquals(2, map.size());

    // Check entries
    // Since TreeMap is sorted by key (cumulative prob)
    // First entry should be either A or C depending on iteration order (HashMap is
    // undefined order)
    // BUT we iterate values() of ImmutableMap. The order of values() in
    // ImmutableMap matches iteration?
    // ImmutableMap usually preserves insertion order?
    // "A", "B", "C" -> A first.
    // A: 0.6875 -> A
    // C: 0.6875 + 0.3125 = 1.0 -> C

    // OR C first:
    // C: 0.3125 -> C
    // A: 0.3125 + 0.6875 = 1.0 -> A

    // Let's verify sum is 1.0 (approx)
    assertEquals(1.0, map.lastKey(), 0.0001);

    boolean foundA = false;
    boolean foundC = false;

    for (DataGeneratorTable t : map.values()) {
      if (t.name().equals("A")) {
        foundA = true;
      }
      if (t.name().equals("C")) {
        foundC = true;
      }
    }

    assertTrue("Should contain A", foundA);
    assertTrue("Should contain C", foundC);

    // Verify weights roughly
    // keys are cumulative.
    Double[] keys = map.keySet().toArray(new Double[0]);
    double firstProb = keys[0];

    // New Logic (Root QPS Only):
    // Root A (10 QPS). Root C (50 QPS).
    // Total Root QPS = 60.
    // Weight A = 10. Prob = 10/60 = 0.1666...
    // Weight C = 50. Prob = 50/60 = 0.8333...

    DataGeneratorTable firstTable = map.get(keys[0]);
    double expectedFirstProb = firstTable.name().equals("A") ? 10.0 / 60.0 : 50.0 / 60.0;

    assertEquals(expectedFirstProb, firstProb, 0.0001);
  }
}
