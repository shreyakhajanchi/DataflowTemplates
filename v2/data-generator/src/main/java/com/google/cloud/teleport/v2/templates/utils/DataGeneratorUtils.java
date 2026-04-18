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
import java.math.BigDecimal;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.TimeUnit;
import org.apache.beam.sdk.schemas.Schema;
import org.joda.time.Instant;

/** Common utilities for data generation. */
public class DataGeneratorUtils {

  /**
   * Generates a random value for the given column using the provided Faker instance.
   *
   * @param column The column definition.
   * @param faker The Faker instance to use.
   * @return The generated value.
   */
  public static Object generateValue(DataGeneratorColumn column, Faker faker) {
    LogicalType type = column.logicalType();
    Long size = column.size();

    switch (type) {
      case STRING:
        int len = (size != null && size > 0 && size < 1000) ? size.intValue() : 20;
        return faker.lorem().characters(len);
      case INT64:
        return faker.number().numberBetween(1_000_000_000L, 2_147_483_647L);
      case FLOAT64:
        int scale = column.scale() != null ? column.scale() : 2;
        int precision = column.precision() != null ? column.precision() : 15;
        double maxVal = Math.pow(10, precision - scale) - 1.0 / Math.pow(10, scale);
        double minVal = -maxVal;
        return faker.number().randomDouble(scale, (long) minVal, (long) maxVal);
      case NUMERIC:
        return new BigDecimal(faker.number().randomNumber());
      case BOOLEAN:
        return faker.bool().bool();
      case BYTES:
        int byteLen = (size != null && size > 0 && size < 1000) ? size.intValue() : 20;
        return faker.lorem().characters(byteLen).getBytes(StandardCharsets.UTF_8);
      case DATE:
        java.util.Date pastDate = faker.date().past(365 * 2, TimeUnit.DAYS);
        java.util.Calendar cal = java.util.Calendar.getInstance();
        cal.setTime(pastDate);
        cal.set(java.util.Calendar.HOUR_OF_DAY, 0);
        cal.set(java.util.Calendar.MINUTE, 0);
        cal.set(java.util.Calendar.SECOND, 0);
        cal.set(java.util.Calendar.MILLISECOND, 0);
        return new Instant(cal.getTimeInMillis());
      case TIMESTAMP:
        return new Instant(faker.date().past(365 * 2, TimeUnit.DAYS).getTime());
      case JSON:
        return "{\"id\": " + faker.number().randomNumber() + "}";
      case UUID:
        return java.util.UUID.randomUUID().toString();
      case ENUM:
        java.util.List<String> enumVals = column.enumValues();
        if (enumVals != null && !enumVals.isEmpty()) {
          return faker.options().option(enumVals.toArray(new String[0]));
        }
        return "UNKNOWN_ENUM";
      case ARRAY:
        int arraySize = faker.number().numberBetween(1, 5);
        java.util.List<Object> arrayList = new java.util.ArrayList<>();
        LogicalType elementType = column.elementType();
        if (elementType == null) {
          elementType = LogicalType.STRING;
        }
        DataGeneratorColumn elementCol =
            DataGeneratorColumn.builder()
                .name(column.name() + "_elem")
                .logicalType(elementType)
                .isNullable(true)
                .isPrimaryKey(false)
                .isGenerated(false)
                .originalType("")
                .build();
        for (int i = 0; i < arraySize; i++) {
          arrayList.add(generateValue(elementCol, faker));
        }
        return arrayList;
      default:
        return "unknown";
    }
  }

  public static Schema.FieldType mapToBeamFieldType(LogicalType logicalType) {
    return mapToBeamFieldType(logicalType, null);
  }

  public static Schema.FieldType mapToBeamFieldType(
      LogicalType logicalType, LogicalType elementType) {
    switch (logicalType) {
      case STRING:
      case JSON:
      case UUID:
      case ENUM:
        return Schema.FieldType.STRING;
      case INT64:
        return Schema.FieldType.INT64;
      case FLOAT64:
        return Schema.FieldType.DOUBLE;
      case NUMERIC:
        return Schema.FieldType.DECIMAL;
      case BOOLEAN:
        return Schema.FieldType.BOOLEAN;
      case BYTES:
        return Schema.FieldType.BYTES;
      case DATE:
      case TIMESTAMP:
        return Schema.FieldType.DATETIME;
      case ARRAY:
        if (elementType == null) {
          elementType = LogicalType.STRING;
        }
        return Schema.FieldType.iterable(mapToBeamFieldType(elementType, null));
      default:
        return Schema.FieldType.STRING;
    }
  }
}
