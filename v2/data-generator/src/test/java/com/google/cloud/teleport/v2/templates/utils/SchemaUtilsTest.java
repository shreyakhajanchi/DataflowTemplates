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
package com.google.cloud.teleport.v2.templates.utils;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import com.google.cloud.teleport.v2.templates.model.DataGeneratorForeignKey;
import com.google.cloud.teleport.v2.templates.model.DataGeneratorSchema;
import com.google.cloud.teleport.v2.templates.model.DataGeneratorTable;
import com.google.cloud.teleport.v2.templates.model.SinkDialect;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

@RunWith(JUnit4.class)
public class SchemaUtilsTest {

  @Test
  public void testDAGConstructionSimple() {
    // Parent -> Child
    DataGeneratorTable parent =
        DataGeneratorTable.builder()
            .name("Parent")
            .columns(ImmutableList.of())
            .primaryKeys(ImmutableList.of())
            .foreignKeys(ImmutableList.of())
            .uniqueKeys(ImmutableList.of())
            .qps(10)
            .isRoot(false) // Default false, should be set to true
            .children(ImmutableList.of())
            .build();

    DataGeneratorTable child =
        DataGeneratorTable.builder()
            .name("Child")
            .columns(ImmutableList.of())
            .primaryKeys(ImmutableList.of())
            .foreignKeys(
                ImmutableList.of(
                    DataGeneratorForeignKey.builder()
                        .name("fk_parent")
                        .keyColumns(ImmutableList.of("parentId"))
                        .referencedTable("Parent")
                        .referencedColumns(ImmutableList.of("id"))
                        .build()))
            .uniqueKeys(ImmutableList.of())
            .qps(10)
            .isRoot(true) // Should be set to false
            .children(ImmutableList.of())
            .build();

    DataGeneratorSchema schema =
        DataGeneratorSchema.builder()
            .dialect(SinkDialect.GOOGLE_STANDARD_SQL)
            .tables(ImmutableMap.of("Parent", parent, "Child", child))
            .build();

    DataGeneratorSchema dagSchema = SchemaUtils.setSchemaDAG(schema);

    DataGeneratorTable newParent = dagSchema.tables().get("Parent");
    DataGeneratorTable newChild = dagSchema.tables().get("Child");

    assertTrue(newParent.isRoot());
    assertFalse(newChild.isRoot());
    assertEquals(1, newParent.children().size());
    assertEquals("Child", newParent.children().get(0));
    assertEquals(0, newChild.children().size());
  }

  @Test
  public void testDAGConstructionMultiParent() {
    // P1 (100 QPS) -> Child
    // P2 (10 QPS) -> Child
    // Child should be child of P2 (Least QPS)

    DataGeneratorTable p1 =
        DataGeneratorTable.builder()
            .name("P1")
            .columns(ImmutableList.of())
            .primaryKeys(ImmutableList.of())
            .foreignKeys(ImmutableList.of())
            .uniqueKeys(ImmutableList.of())
            .qps(100)
            .isRoot(false)
            .build();

    DataGeneratorTable p2 =
        DataGeneratorTable.builder()
            .name("P2")
            .columns(ImmutableList.of())
            .primaryKeys(ImmutableList.of())
            .foreignKeys(ImmutableList.of())
            .uniqueKeys(ImmutableList.of())
            .qps(10)
            .isRoot(false)
            .build();

    DataGeneratorTable child =
        DataGeneratorTable.builder()
            .name("Child")
            .columns(ImmutableList.of())
            .primaryKeys(ImmutableList.of())
            .foreignKeys(
                ImmutableList.of(
                    DataGeneratorForeignKey.builder()
                        .name("fk_p1")
                        .keyColumns(ImmutableList.of("p1Id"))
                        .referencedTable("P1")
                        .referencedColumns(ImmutableList.of("id"))
                        .build(),
                    DataGeneratorForeignKey.builder()
                        .name("fk_p2")
                        .keyColumns(ImmutableList.of("p2Id"))
                        .referencedTable("P2")
                        .referencedColumns(ImmutableList.of("id"))
                        .build()))
            .uniqueKeys(ImmutableList.of())
            .qps(10)
            .isRoot(false)
            .build();

    DataGeneratorSchema schema =
        DataGeneratorSchema.builder()
            .dialect(SinkDialect.GOOGLE_STANDARD_SQL)
            .tables(ImmutableMap.of("P1", p1, "P2", p2, "Child", child))
            .build();

    DataGeneratorSchema dagSchema = SchemaUtils.setSchemaDAG(schema);

    DataGeneratorTable newP1 = dagSchema.tables().get("P1");
    DataGeneratorTable newP2 = dagSchema.tables().get("P2");
    DataGeneratorTable newChild = dagSchema.tables().get("Child");

    assertTrue(newP1.isRoot());
    assertTrue(newP2.isRoot());
    assertFalse(newChild.isRoot());

    // P1 should NOT have Child in children list
    assertEquals(0, newP1.children().size());

    // P2 SHOULD have Child in children list
    assertEquals(1, newP2.children().size());
    assertEquals("Child", newP2.children().get(0));
  }
}
