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
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import java.util.List;
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
            .insertQps(10)
            .setRoot(false) // Default false, should be set to true
            .childTables(ImmutableList.of())
            .updateQps(0)
            .deleteQps(0)
            .recordsPerTick(1.0)
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
            .insertQps(10)
            .setRoot(true) // Should be set to false
            .childTables(ImmutableList.of())
            .updateQps(0)
            .deleteQps(0)
            .recordsPerTick(1.0)
            .build();

    DataGeneratorSchema schema =
        DataGeneratorSchema.builder()
            .tables(ImmutableMap.of("Parent", parent, "Child", child))
            .build();

    DataGeneratorSchema dagSchema = SchemaUtils.generateSchemaDAG(schema);

    DataGeneratorTable newParent = dagSchema.getTables().get("Parent");
    DataGeneratorTable newChild = dagSchema.getTables().get("Child");

    assertTrue(newParent.getRoot());
    assertFalse(newChild.getRoot());
    assertEquals(1, newParent.getChildTables().size());
    assertEquals("Child", newParent.getChildTables().get(0));
    assertEquals(0, newChild.getChildTables().size());
  }

  @Test
  public void testDAGConstructionMultiParentChain() {
    // P1 (10 QPS) -> P2 (100 QPS) -> Child
    DataGeneratorTable p1 =
        DataGeneratorTable.builder()
            .name("P1")
            .columns(ImmutableList.of())
            .primaryKeys(ImmutableList.of())
            .foreignKeys(ImmutableList.of())
            .uniqueKeys(ImmutableList.of())
            .insertQps(10)
            .setRoot(false)
            .updateQps(0)
            .deleteQps(0)
            .recordsPerTick(1.0)
            .build();

    DataGeneratorTable p2 =
        DataGeneratorTable.builder()
            .name("P2")
            .columns(ImmutableList.of())
            .primaryKeys(ImmutableList.of())
            .foreignKeys(ImmutableList.of())
            .uniqueKeys(ImmutableList.of())
            .insertQps(100)
            .setRoot(false)
            .updateQps(0)
            .deleteQps(0)
            .recordsPerTick(1.0)
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
            .insertQps(200)
            .setRoot(false)
            .updateQps(0)
            .deleteQps(0)
            .recordsPerTick(1.0)
            .build();

    DataGeneratorSchema schema =
        DataGeneratorSchema.builder()
            .tables(ImmutableMap.of("P1", p1, "P2", p2, "Child", child))
            .build();

    DataGeneratorSchema dagSchema = SchemaUtils.generateSchemaDAG(schema);

    DataGeneratorTable newP1 = dagSchema.getTables().get("P1");
    DataGeneratorTable newP2 = dagSchema.getTables().get("P2");
    DataGeneratorTable newChild = dagSchema.getTables().get("Child");

    assertTrue(newP1.getRoot());
    assertFalse(newP2.getRoot());
    assertFalse(newChild.getRoot());

    // P1 should have P2
    assertEquals(1, newP1.getChildTables().size());
    assertEquals("P2", newP1.getChildTables().get(0));

    // P2 should have Child
    assertEquals(1, newP2.getChildTables().size());
    assertEquals("Child", newP2.getChildTables().get(0));

    assertEquals(0, newChild.getChildTables().size());
  }

  @Test
  public void testDAGConstructionInterleaving() {
    // InterleavedParent -> Child (interleaved)
    // OtherParent (1 QPS) -> Child (FK)

    DataGeneratorTable interleavedParent =
        DataGeneratorTable.builder()
            .name("InterleavedParent")
            .insertQps(100)
            .columns(ImmutableList.of())
            .primaryKeys(ImmutableList.of())
            .foreignKeys(ImmutableList.of())
            .uniqueKeys(ImmutableList.of())
            .setRoot(false)
            .updateQps(0)
            .deleteQps(0)
            .recordsPerTick(1.0)
            .build();
    DataGeneratorTable otherParent =
        DataGeneratorTable.builder()
            .name("OtherParent")
            .insertQps(1)
            .columns(ImmutableList.of())
            .primaryKeys(ImmutableList.of())
            .foreignKeys(ImmutableList.of())
            .uniqueKeys(ImmutableList.of())
            .setRoot(false)
            .updateQps(0)
            .deleteQps(0)
            .recordsPerTick(1.0)
            .build();

    DataGeneratorTable child =
        DataGeneratorTable.builder()
            .name("Child")
            .interleavedInTable("InterleavedParent")
            .foreignKeys(
                ImmutableList.of(
                    DataGeneratorForeignKey.builder()
                        .name("fk_other")
                        .keyColumns(ImmutableList.of("otherId"))
                        .referencedTable("OtherParent")
                        .referencedColumns(ImmutableList.of("id"))
                        .build()))
            .insertQps(10)
            .columns(ImmutableList.of())
            .primaryKeys(ImmutableList.of())
            .uniqueKeys(ImmutableList.of())
            .setRoot(false)
            .updateQps(0)
            .deleteQps(0)
            .recordsPerTick(1.0)
            .build();

    DataGeneratorSchema schema =
        DataGeneratorSchema.builder()
            .tables(
                ImmutableMap.of(
                    "InterleavedParent",
                    interleavedParent,
                    "OtherParent",
                    otherParent,
                    "Child",
                    child))
            .build();

    DataGeneratorSchema dagSchema = SchemaUtils.generateSchemaDAG(schema);

    assertFalse(dagSchema.getTables().get("InterleavedParent").getRoot());
    assertTrue(dagSchema.getTables().get("OtherParent").getRoot());
    assertFalse(dagSchema.getTables().get("Child").getRoot());

    assertEquals(1, dagSchema.getTables().get("OtherParent").getChildTables().size());
    assertEquals(
        "InterleavedParent", dagSchema.getTables().get("OtherParent").getChildTables().get(0));

    assertEquals(1, dagSchema.getTables().get("InterleavedParent").getChildTables().size());
    assertEquals("Child", dagSchema.getTables().get("InterleavedParent").getChildTables().get(0));
  }

  @Test
  public void testDAGConstructionGrandChild() {
    // P1 -> P2 -> C1 -> GC1
    // P2 -> C2
    // P1 (10), P2 (100), C1 (20), C2 (30), GC1 (40)
    // C1 now has parents P1 and P2

    DataGeneratorTable p1 =
        DataGeneratorTable.builder()
            .name("P1")
            .insertQps(10)
            .columns(ImmutableList.of())
            .primaryKeys(ImmutableList.of())
            .foreignKeys(ImmutableList.of())
            .uniqueKeys(ImmutableList.of())
            .setRoot(false)
            .updateQps(0)
            .deleteQps(0)
            .recordsPerTick(1.0)
            .build();
    DataGeneratorTable p2 =
        DataGeneratorTable.builder()
            .name("P2")
            .insertQps(100)
            .columns(ImmutableList.of())
            .primaryKeys(ImmutableList.of())
            .foreignKeys(ImmutableList.of())
            .uniqueKeys(ImmutableList.of())
            .setRoot(false)
            .updateQps(0)
            .deleteQps(0)
            .recordsPerTick(1.0)
            .build();
    DataGeneratorTable c1 =
        DataGeneratorTable.builder()
            .name("C1")
            .insertQps(20)
            .foreignKeys(
                ImmutableList.of(
                    DataGeneratorForeignKey.builder()
                        .name("fk_p1")
                        .keyColumns(ImmutableList.of("p1Id"))
                        .referencedTable("P1")
                        .referencedColumns(ImmutableList.of("id"))
                        .build(),
                    DataGeneratorForeignKey.builder()
                        .name("fk_p2_c1")
                        .keyColumns(ImmutableList.of("p2Id"))
                        .referencedTable("P2")
                        .referencedColumns(ImmutableList.of("id"))
                        .build()))
            .columns(ImmutableList.of())
            .primaryKeys(ImmutableList.of())
            .uniqueKeys(ImmutableList.of())
            .setRoot(false)
            .updateQps(0)
            .deleteQps(0)
            .recordsPerTick(1.0)
            .build();
    DataGeneratorTable c2 =
        DataGeneratorTable.builder()
            .name("C2")
            .insertQps(30)
            .foreignKeys(
                ImmutableList.of(
                    DataGeneratorForeignKey.builder()
                        .name("fk_p2")
                        .keyColumns(ImmutableList.of("p2Id"))
                        .referencedTable("P2")
                        .referencedColumns(ImmutableList.of("id"))
                        .build()))
            .columns(ImmutableList.of())
            .primaryKeys(ImmutableList.of())
            .uniqueKeys(ImmutableList.of())
            .setRoot(false)
            .updateQps(0)
            .deleteQps(0)
            .recordsPerTick(1.0)
            .build();
    DataGeneratorTable gc1 =
        DataGeneratorTable.builder()
            .name("GC1")
            .insertQps(40)
            .foreignKeys(
                ImmutableList.of(
                    DataGeneratorForeignKey.builder()
                        .name("fk_c1")
                        .keyColumns(ImmutableList.of("c1Id"))
                        .referencedTable("C1")
                        .referencedColumns(ImmutableList.of("id"))
                        .build()))
            .columns(ImmutableList.of())
            .primaryKeys(ImmutableList.of())
            .uniqueKeys(ImmutableList.of())
            .setRoot(false)
            .updateQps(0)
            .deleteQps(0)
            .recordsPerTick(1.0)
            .build();

    DataGeneratorSchema schema =
        DataGeneratorSchema.builder()
            .tables(ImmutableMap.of("P1", p1, "P2", p2, "C1", c1, "C2", c2, "GC1", gc1))
            .build();

    DataGeneratorSchema dagSchema = SchemaUtils.generateSchemaDAG(schema);

    assertTrue(dagSchema.getTables().get("P1").getRoot());
    assertFalse(
        dagSchema.getTables().get("P2").getRoot()); // P2 is now a sequence child of P1 for C1
    assertFalse(dagSchema.getTables().get("C1").getRoot());
    assertFalse(dagSchema.getTables().get("C2").getRoot());
    assertFalse(dagSchema.getTables().get("GC1").getRoot());

    assertEquals(1, dagSchema.getTables().get("P1").getChildTables().size());
    assertEquals(
        "P2", dagSchema.getTables().get("P1").getChildTables().get(0)); // P1 -> P2 sequence

    assertEquals(2, dagSchema.getTables().get("P2").getChildTables().size());
    assertTrue(
        dagSchema.getTables().get("P2").getChildTables().contains("C1")); // P2 -> C1 sequence
    assertTrue(dagSchema.getTables().get("P2").getChildTables().contains("C2")); // P2 -> C2 direct

    assertEquals(1, dagSchema.getTables().get("C1").getChildTables().size());
    assertEquals("GC1", dagSchema.getTables().get("C1").getChildTables().get(0));
    assertEquals(0, dagSchema.getTables().get("C2").getChildTables().size());
    assertEquals(0, dagSchema.getTables().get("GC1").getChildTables().size());
  }

  @Test
  public void testDAGConstructionMultiParentChainComplex() {
    DataGeneratorTable organizations =
        DataGeneratorTable.builder()
            .name("Organizations")
            .insertQps(500)
            .columns(ImmutableList.of())
            .primaryKeys(ImmutableList.of())
            .foreignKeys(ImmutableList.of())
            .uniqueKeys(ImmutableList.of())
            .build();

    DataGeneratorTable xyz =
        DataGeneratorTable.builder()
            .name("xyz")
            .insertQps(400)
            .columns(ImmutableList.of())
            .primaryKeys(ImmutableList.of())
            .foreignKeys(ImmutableList.of())
            .uniqueKeys(ImmutableList.of())
            .build();

    DataGeneratorTable departments =
        DataGeneratorTable.builder()
            .name("Departments")
            .insertQps(200)
            .columns(ImmutableList.of())
            .primaryKeys(ImmutableList.of())
            .foreignKeys(
                ImmutableList.of(
                    DataGeneratorForeignKey.builder()
                        .name("fk_dept_org")
                        .keyColumns(ImmutableList.of("OrgId"))
                        .referencedTable("Organizations")
                        .referencedColumns(ImmutableList.of("OrgId"))
                        .build()))
            .uniqueKeys(ImmutableList.of())
            .build();

    DataGeneratorTable employees =
        DataGeneratorTable.builder()
            .name("EmployeeAssignments")
            .insertQps(100)
            .columns(ImmutableList.of())
            .primaryKeys(ImmutableList.of())
            .foreignKeys(
                ImmutableList.of(
                    DataGeneratorForeignKey.builder()
                        .name("fk_emp_xyz")
                        .keyColumns(ImmutableList.of("xyzId"))
                        .referencedTable("xyz")
                        .referencedColumns(ImmutableList.of("xyzId"))
                        .build()))
            .uniqueKeys(ImmutableList.of())
            .build();

    DataGeneratorTable projects =
        DataGeneratorTable.builder()
            .name("Projects")
            .insertQps(50)
            .columns(ImmutableList.of())
            .primaryKeys(ImmutableList.of())
            .foreignKeys(
                ImmutableList.of(
                    DataGeneratorForeignKey.builder()
                        .name("fk_proj_dept")
                        .keyColumns(ImmutableList.of("DeptCode"))
                        .referencedTable("Departments")
                        .referencedColumns(ImmutableList.of("DeptCode"))
                        .build(),
                    DataGeneratorForeignKey.builder()
                        .name("fk_proj_emp")
                        .keyColumns(ImmutableList.of("EmpId"))
                        .referencedTable("EmployeeAssignments")
                        .referencedColumns(ImmutableList.of("EmpId"))
                        .build()))
            .uniqueKeys(ImmutableList.of())
            .build();

    DataGeneratorSchema schema =
        DataGeneratorSchema.builder()
            .tables(
                ImmutableMap.of(
                    "Organizations",
                    organizations,
                    "xyz",
                    xyz,
                    "Departments",
                    departments,
                    "EmployeeAssignments",
                    employees,
                    "Projects",
                    projects))
            .build();

    DataGeneratorSchema dagSchema = SchemaUtils.generateSchemaDAG(schema);

    DataGeneratorTable newOrg = dagSchema.getTables().get("Organizations");
    DataGeneratorTable newXyz = dagSchema.getTables().get("xyz");
    DataGeneratorTable newDept = dagSchema.getTables().get("Departments");
    DataGeneratorTable newEmp = dagSchema.getTables().get("EmployeeAssignments");
    DataGeneratorTable newProj = dagSchema.getTables().get("Projects");

    assertTrue(newXyz.getRoot());
    assertFalse(newEmp.getRoot());
    assertFalse(newOrg.getRoot());
    assertFalse(newDept.getRoot());
    assertFalse(newProj.getRoot());

    assertEquals(1, newXyz.getChildTables().size());
    assertEquals("EmployeeAssignments", newXyz.getChildTables().get(0));

    assertEquals(1, newEmp.getChildTables().size());
    assertEquals("Organizations", newEmp.getChildTables().get(0));

    assertEquals(1, newOrg.getChildTables().size());
    assertEquals("Departments", newOrg.getChildTables().get(0));

    assertEquals(1, newDept.getChildTables().size());
    assertEquals("Projects", newDept.getChildTables().get(0));
  }

  @Test
  public void testBuildInsertTopoOrderRootsBeforeChildren() {
    DataGeneratorTable parent =
        DataGeneratorTable.builder()
            .name("Parent")
            .columns(ImmutableList.of())
            .primaryKeys(ImmutableList.of())
            .foreignKeys(ImmutableList.of())
            .uniqueKeys(ImmutableList.of())
            .insertQps(1)
            .setRoot(true)
            .childTables(ImmutableList.of("Child"))
            .build();
    DataGeneratorTable child =
        DataGeneratorTable.builder()
            .name("Child")
            .columns(ImmutableList.of())
            .primaryKeys(ImmutableList.of())
            .foreignKeys(ImmutableList.of())
            .uniqueKeys(ImmutableList.of())
            .insertQps(1)
            .setRoot(false)
            .build();

    DataGeneratorSchema schema =
        DataGeneratorSchema.builder()
            .tables(ImmutableMap.of("Parent", parent, "Child", child))
            .build();

    List<String> order = SchemaUtils.buildInsertTopoOrder(schema);
    assertEquals(2, order.size());
    assertEquals("Parent", order.get(0));
    assertEquals("Child", order.get(1));
  }

  @Test
  public void testBuildInsertTopoOrderMultipleRootsSortedByName() {
    DataGeneratorSchema schema =
        DataGeneratorSchema.builder()
            .tables(
                ImmutableMap.of(
                    "B",
                    DataGeneratorTable.builder()
                        .name("B")
                        .columns(ImmutableList.of())
                        .primaryKeys(ImmutableList.of())
                        .foreignKeys(ImmutableList.of())
                        .uniqueKeys(ImmutableList.of())
                        .insertQps(1)
                        .setRoot(true)
                        .build(),
                    "A",
                    DataGeneratorTable.builder()
                        .name("A")
                        .columns(ImmutableList.of())
                        .primaryKeys(ImmutableList.of())
                        .foreignKeys(ImmutableList.of())
                        .uniqueKeys(ImmutableList.of())
                        .insertQps(1)
                        .setRoot(true)
                        .build(),
                    "C",
                    DataGeneratorTable.builder()
                        .name("C")
                        .columns(ImmutableList.of())
                        .primaryKeys(ImmutableList.of())
                        .foreignKeys(ImmutableList.of())
                        .uniqueKeys(ImmutableList.of())
                        .insertQps(1)
                        .setRoot(true)
                        .build()))
            .build();

    List<String> order = SchemaUtils.buildInsertTopoOrder(schema);
    assertEquals(3, order.size());
    assertEquals("A", order.get(0));
    assertEquals("B", order.get(1));
    assertEquals("C", order.get(2));
  }

  @Test
  public void testSetSchemaDAG_overridesChildDeleteQpsWhenAncestorHasDeletes() {
    // Grandparent (deleteQps = 5) -> Parent (deleteQps = 0) -> Child (deleteQps = 10)
    DataGeneratorTable grandparent =
        DataGeneratorTable.builder()
            .name("Grandparent")
            .columns(ImmutableList.of())
            .primaryKeys(ImmutableList.of())
            .foreignKeys(ImmutableList.of())
            .uniqueKeys(ImmutableList.of())
            .insertQps(10)
            .deleteQps(5)
            .build();

    DataGeneratorTable parent =
        DataGeneratorTable.builder()
            .name("Parent")
            .columns(ImmutableList.of())
            .primaryKeys(ImmutableList.of())
            .foreignKeys(
                ImmutableList.of(
                    DataGeneratorForeignKey.builder()
                        .name("fk_gp")
                        .keyColumns(ImmutableList.of("gpId"))
                        .referencedTable("Grandparent")
                        .referencedColumns(ImmutableList.of("id"))
                        .build()))
            .uniqueKeys(ImmutableList.of())
            .insertQps(10)
            .deleteQps(0)
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
            .insertQps(10)
            .deleteQps(10)
            .build();

    DataGeneratorSchema schema =
        DataGeneratorSchema.builder()
            .tables(ImmutableMap.of("Grandparent", grandparent, "Parent", parent, "Child", child))
            .build();

    DataGeneratorSchema dagSchema = SchemaUtils.generateSchemaDAG(schema);

    assertEquals(Integer.valueOf(5), dagSchema.getTables().get("Grandparent").getDeleteQps());
    assertEquals(Integer.valueOf(0), dagSchema.getTables().get("Parent").getDeleteQps());
    assertEquals(
        Integer.valueOf(0),
        dagSchema.getTables().get("Child").getDeleteQps()); // Overwritten due to grandparent!
  }

  @Test(expected = IllegalStateException.class)
  public void testCircularDependencyThrowsException() {
    DataGeneratorTable t1 =
        DataGeneratorTable.builder()
            .name("T1")
            .columns(ImmutableList.of())
            .primaryKeys(ImmutableList.of())
            .foreignKeys(
                ImmutableList.of(
                    DataGeneratorForeignKey.builder()
                        .name("fk_t1_t2")
                        .keyColumns(ImmutableList.of("t2Id"))
                        .referencedTable("T2")
                        .referencedColumns(ImmutableList.of("id"))
                        .build()))
            .uniqueKeys(ImmutableList.of())
            .insertQps(10)
            .build();

    DataGeneratorTable t2 =
        DataGeneratorTable.builder()
            .name("T2")
            .columns(ImmutableList.of())
            .primaryKeys(ImmutableList.of())
            .foreignKeys(
                ImmutableList.of(
                    DataGeneratorForeignKey.builder()
                        .name("fk_t2_t1")
                        .keyColumns(ImmutableList.of("t1Id"))
                        .referencedTable("T1")
                        .referencedColumns(ImmutableList.of("id"))
                        .build()))
            .uniqueKeys(ImmutableList.of())
            .insertQps(10)
            .build();

    DataGeneratorSchema schema =
        DataGeneratorSchema.builder().tables(ImmutableMap.of("T1", t1, "T2", t2)).build();

    SchemaUtils.buildInsertTopoOrder(schema);
  }
}
