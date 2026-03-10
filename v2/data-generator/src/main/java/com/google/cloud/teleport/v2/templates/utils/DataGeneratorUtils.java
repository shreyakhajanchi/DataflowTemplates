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
        return faker.number().randomNumber();
      case FLOAT64:
        return faker.number().randomDouble(2, Short.MIN_VALUE, Short.MAX_VALUE);
      case NUMERIC:
        return new BigDecimal(faker.number().randomNumber());
      case BOOLEAN:
        return faker.bool().bool();
      case BYTES:
        int byteLen = (size != null && size > 0 && size < 1000) ? size.intValue() : 20;
        return faker.lorem().characters(byteLen).getBytes(StandardCharsets.UTF_8);
      case DATE:
      case TIMESTAMP:
        return new Instant(faker.date().past(365 * 2, TimeUnit.DAYS).getTime());
      case JSON:
        return "{\"id\": " + faker.number().randomNumber() + "}";
      default:
        return "unknown";
    }
  }

  /**
   * Maps a {@link LogicalType} to a Beam {@link Schema.FieldType}.
   *
   * @param logicalType The logical type.
   * @return The corresponding Beam FieldType.
   */
  public static Schema.FieldType mapToBeamFieldType(LogicalType logicalType) {
    switch (logicalType) {
      case STRING:
      case JSON:
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
      default:
        return Schema.FieldType.STRING;
    }
  }
}
