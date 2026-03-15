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
package com.google.cloud.teleport.v2.templates.mysql;

import static org.junit.Assert.assertEquals;

import com.google.cloud.teleport.v2.templates.model.LogicalType;
import com.google.cloud.teleport.v2.templates.model.SinkDialect;
import org.junit.Test;

public class MySqlTypeMapperTest {

  private final MySqlTypeMapper mapper = new MySqlTypeMapper();

  @Test
  public void testMySqlMapping() {
    verifyMapping("BIT", LogicalType.BOOLEAN);
    verifyMapping("BOOLEAN", LogicalType.BOOLEAN);
    verifyMapping("BOOL", LogicalType.BOOLEAN);
    verifyMapping("TINYINT", LogicalType.INT64);
    verifyMapping("SMALLINT", LogicalType.INT64);
    verifyMapping("INTEGER", LogicalType.INT64);
    verifyMapping("INT", LogicalType.INT64);
    verifyMapping("BIGINT", LogicalType.INT64);
    verifyMapping("FLOAT", LogicalType.FLOAT64);
    verifyMapping("REAL", LogicalType.FLOAT64);
    verifyMapping("DOUBLE", LogicalType.FLOAT64);
    verifyMapping("NUMERIC", LogicalType.NUMERIC);
    verifyMapping("DECIMAL", LogicalType.NUMERIC);
    verifyMapping("CHAR", LogicalType.STRING);
    verifyMapping("VARCHAR(255)", LogicalType.STRING);
    verifyMapping("TEXT", LogicalType.STRING);
    verifyMapping("MEDIUMTEXT", LogicalType.STRING);
    verifyMapping("LONGTEXT", LogicalType.STRING);
    verifyMapping("BINARY", LogicalType.BYTES);
    verifyMapping("VARBINARY", LogicalType.BYTES);
    verifyMapping("BLOB", LogicalType.BYTES);
    verifyMapping("DATE", LogicalType.DATE);
    verifyMapping("TIME", LogicalType.TIMESTAMP);
    verifyMapping("DATETIME", LogicalType.TIMESTAMP);
    verifyMapping("TIMESTAMP", LogicalType.TIMESTAMP);
    verifyMapping("JSON", LogicalType.JSON);
    verifyMapping("UNKNOWN", LogicalType.STRING);
    verifyMapping(null, LogicalType.STRING);
  }

  private void verifyMapping(String inputType, LogicalType expected) {
    assertEquals(
        "Failed for type: " + inputType,
        expected,
        mapper.getLogicalType(inputType, SinkDialect.MYSQL));
  }
}
