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
package com.google.cloud.teleport.v2.templates.spanner;

import static org.junit.Assert.assertEquals;

import com.google.cloud.spanner.Dialect;
import com.google.cloud.teleport.v2.templates.model.LogicalType;
import org.junit.Test;

public class SpannerTypeMapperTest {

  private final SpannerTypeMapper mapper = new SpannerTypeMapper();

  @Test
  public void testGoogleStandardSqlMapping() {
    // Basic types
    verifyMapping(Dialect.GOOGLE_STANDARD_SQL, "BOOL", LogicalType.BOOLEAN);
    verifyMapping(Dialect.GOOGLE_STANDARD_SQL, "INT64", LogicalType.INT64);
    verifyMapping(Dialect.GOOGLE_STANDARD_SQL, "FLOAT64", LogicalType.FLOAT64);
    verifyMapping(Dialect.GOOGLE_STANDARD_SQL, "FLOAT32", LogicalType.FLOAT64);
    verifyMapping(Dialect.GOOGLE_STANDARD_SQL, "STRING", LogicalType.STRING);
    verifyMapping(Dialect.GOOGLE_STANDARD_SQL, "BYTES", LogicalType.BYTES);
    verifyMapping(Dialect.GOOGLE_STANDARD_SQL, "DATE", LogicalType.DATE);
    verifyMapping(Dialect.GOOGLE_STANDARD_SQL, "TIMESTAMP", LogicalType.TIMESTAMP);
    verifyMapping(Dialect.GOOGLE_STANDARD_SQL, "NUMERIC", LogicalType.NUMERIC);
    verifyMapping(Dialect.GOOGLE_STANDARD_SQL, "BIGNUMERIC", LogicalType.NUMERIC);
    verifyMapping(Dialect.GOOGLE_STANDARD_SQL, "JSON", LogicalType.JSON);

    // Parameterized types
    verifyMapping(Dialect.GOOGLE_STANDARD_SQL, "STRING(MAX)", LogicalType.STRING);
    verifyMapping(Dialect.GOOGLE_STANDARD_SQL, "STRING(100)", LogicalType.STRING);
    verifyMapping(Dialect.GOOGLE_STANDARD_SQL, "BYTES(MAX)", LogicalType.BYTES);
    verifyMapping(Dialect.GOOGLE_STANDARD_SQL, "BYTES(100)", LogicalType.BYTES);
    verifyMapping(Dialect.GOOGLE_STANDARD_SQL, "NUMERIC(10,2)", LogicalType.NUMERIC);

    // Complex types (sanitization strips parameters)
    verifyMapping(Dialect.GOOGLE_STANDARD_SQL, "ARRAY<INT64>", LogicalType.ARRAY);
    verifyMapping(Dialect.GOOGLE_STANDARD_SQL, "ARRAY<STRING(MAX)>", LogicalType.ARRAY);
    verifyMapping(Dialect.GOOGLE_STANDARD_SQL, "STRUCT<a INT64>", LogicalType.STRUCT);
    verifyMapping(Dialect.GOOGLE_STANDARD_SQL, "STRUCT<a INT64, b STRING>", LogicalType.STRUCT);

    // Case insensitivity
    verifyMapping(Dialect.GOOGLE_STANDARD_SQL, "bool", LogicalType.BOOLEAN);
    verifyMapping(Dialect.GOOGLE_STANDARD_SQL, "Int64", LogicalType.INT64);
    verifyMapping(Dialect.GOOGLE_STANDARD_SQL, "String(Max)", LogicalType.STRING);

    // Unknown/Null
    verifyMapping(Dialect.GOOGLE_STANDARD_SQL, "UNKNOWN", LogicalType.STRING);
    verifyMapping(Dialect.GOOGLE_STANDARD_SQL, null, LogicalType.STRING);
  }

  @Test
  public void testPostgreSqlMapping() {
    verifyMapping(Dialect.POSTGRESQL, "boolean", LogicalType.BOOLEAN);
    verifyMapping(Dialect.POSTGRESQL, "bool", LogicalType.BOOLEAN);
    verifyMapping(Dialect.POSTGRESQL, "bigint", LogicalType.INT64);
    verifyMapping(Dialect.POSTGRESQL, "int8", LogicalType.INT64);
    verifyMapping(Dialect.POSTGRESQL, "real", LogicalType.FLOAT64);
    verifyMapping(Dialect.POSTGRESQL, "double precision", LogicalType.FLOAT64);
    verifyMapping(Dialect.POSTGRESQL, "float4", LogicalType.FLOAT64);
    verifyMapping(Dialect.POSTGRESQL, "float8", LogicalType.FLOAT64);
    verifyMapping(Dialect.POSTGRESQL, "varchar", LogicalType.STRING);
    verifyMapping(Dialect.POSTGRESQL, "text", LogicalType.STRING);
    verifyMapping(Dialect.POSTGRESQL, "character varying", LogicalType.STRING);
    verifyMapping(Dialect.POSTGRESQL, "bytea", LogicalType.BYTES);
    verifyMapping(Dialect.POSTGRESQL, "date", LogicalType.DATE);
    verifyMapping(Dialect.POSTGRESQL, "timestamp with time zone", LogicalType.TIMESTAMP);
    verifyMapping(Dialect.POSTGRESQL, "timestamptz", LogicalType.TIMESTAMP);
    verifyMapping(Dialect.POSTGRESQL, "numeric", LogicalType.NUMERIC);
    verifyMapping(Dialect.POSTGRESQL, "decimal", LogicalType.NUMERIC);
    verifyMapping(Dialect.POSTGRESQL, "jsonb", LogicalType.JSON);

    // Parameterized PG types
    verifyMapping(Dialect.POSTGRESQL, "varchar(255)", LogicalType.STRING);
    verifyMapping(Dialect.POSTGRESQL, "numeric(10,2)", LogicalType.NUMERIC);

    // Case insensitivity
    verifyMapping(Dialect.POSTGRESQL, "VARCHAR", LogicalType.STRING);
    verifyMapping(Dialect.POSTGRESQL, "BigInt", LogicalType.INT64);

    verifyMapping(Dialect.POSTGRESQL, "unknown", LogicalType.STRING);
  }

  private void verifyMapping(Dialect dialect, String inputType, LogicalType expected) {
    assertEquals(
        "Failed for type: " + inputType, expected, mapper.getLogicalType(inputType, dialect));
  }
}
