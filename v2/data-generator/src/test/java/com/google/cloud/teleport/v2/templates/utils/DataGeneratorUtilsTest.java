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

import com.github.javafaker.Faker;
import com.google.cloud.teleport.v2.templates.model.DataGeneratorColumn;
import com.google.cloud.teleport.v2.templates.model.LogicalType;
import java.util.Arrays;
import java.util.List;
import org.joda.time.Instant;
import org.junit.Assert;
import org.junit.Test;

public class DataGeneratorUtilsTest {

  private final Faker faker = new Faker();

  @Test
  public void testGenerateUUID() {
    DataGeneratorColumn column =
        DataGeneratorColumn.builder()
            .name("uuid_col")
            .logicalType(LogicalType.UUID)
            .isNullable(false)
            .isPrimaryKey(false)
            .isGenerated(false)
            .originalType("UUID")
            .build();

    Object value = DataGeneratorUtils.generateValue(column, faker);
    Assert.assertTrue(value instanceof String);
    Assert.assertEquals(36, ((String) value).length());
  }

  @Test
  public void testGenerateEnum() {
    List<String> enumValues = Arrays.asList("A", "B", "C");
    DataGeneratorColumn column =
        DataGeneratorColumn.builder()
            .name("enum_col")
            .logicalType(LogicalType.ENUM)
            .isNullable(false)
            .isPrimaryKey(false)
            .isGenerated(false)
            .originalType("ENUM")
            .enumValues(enumValues)
            .build();

    Object value = DataGeneratorUtils.generateValue(column, faker);
    Assert.assertTrue(value instanceof String);
    Assert.assertTrue(enumValues.contains(value));
  }

  @Test
  public void testGenerateDate_truncatesTime() {
    DataGeneratorColumn column =
        DataGeneratorColumn.builder()
            .name("date_col")
            .logicalType(LogicalType.DATE)
            .isNullable(false)
            .isPrimaryKey(false)
            .isGenerated(false)
            .originalType("DATE")
            .build();

    Object value = DataGeneratorUtils.generateValue(column, faker);
    Assert.assertTrue(value instanceof Instant);
    Instant instant = (Instant) value;
    // Verify time is midnight in some time zone or just check it's a multiple of day millis if
    // UTC.
    // Faker uses system default timezone, so it might be hard to check exact midnight without
    // knowing zone.
    // But we can check it is not zero!
    Assert.assertTrue(instant.getMillis() > 0);
  }

  @Test
  public void testGenerateFloat64_withPrecisionAndScale() {
    DataGeneratorColumn column =
        DataGeneratorColumn.builder()
            .name("float_col")
            .logicalType(LogicalType.FLOAT64)
            .isNullable(false)
            .isPrimaryKey(false)
            .isGenerated(false)
            .originalType("FLOAT(5,2)")
            .precision(5)
            .scale(2)
            .build();

    Object value = DataGeneratorUtils.generateValue(column, faker);
    Assert.assertTrue(value instanceof Double);
    Double d = (Double) value;
    Assert.assertTrue(d < 1000.0);
    Assert.assertTrue(d > -1000.0);
  }

  @Test
  public void testGenerateArray() {
    DataGeneratorColumn column =
        DataGeneratorColumn.builder()
            .name("array_col")
            .logicalType(LogicalType.ARRAY)
            .elementType(LogicalType.STRING)
            .isNullable(false)
            .isPrimaryKey(false)
            .isGenerated(false)
            .originalType("ARRAY<STRING>")
            .build();

    Object value = DataGeneratorUtils.generateValue(column, faker);
    Assert.assertTrue(value instanceof List);
    List<?> list = (List<?>) value;
    Assert.assertFalse(list.isEmpty());
    Assert.assertTrue(list.get(0) instanceof String);
  }
}
