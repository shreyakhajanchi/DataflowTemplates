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

import com.google.cloud.teleport.v2.templates.model.LogicalType;
import com.google.cloud.teleport.v2.templates.model.SinkDialect;
import com.google.cloud.teleport.v2.templates.utils.TypeMapper;
import java.util.Locale;

/** TypeMapper implementation for JDBC types. */
public class MySqlTypeMapper implements TypeMapper {

  @Override
  public LogicalType getLogicalType(String typeName, SinkDialect dialect) {
    if (typeName == null) {
      return LogicalType.STRING;
    }

    String upperType = typeName.toUpperCase(Locale.ROOT);
    // Remove parameters
    if (upperType.contains("(")) {
      upperType = upperType.substring(0, upperType.indexOf("("));
    }

    // Generic JDBC/MySQL mapping
    switch (upperType) {
      case "BIT":
      case "BOOLEAN":
      case "BOOL":
        return LogicalType.BOOLEAN;
      case "TINYINT":
      case "SMALLINT":
      case "INTEGER":
      case "INT":
      case "BIGINT":
        return LogicalType.INT64;
      case "FLOAT":
      case "REAL":
      case "DOUBLE":
      case "NUMERIC":
      case "DECIMAL":
        return LogicalType.NUMERIC; // Or FLOAT64 depending on usage, but NUMERIC is safer
      case "CHAR":
      case "VARCHAR":
      case "LONGVARCHAR":
      case "TEXT":
      case "MEDIUMTEXT":
      case "LONGTEXT":
        return LogicalType.STRING;
      case "BINARY":
      case "VARBINARY":
      case "LONGVARBINARY":
      case "BLOB":
        return LogicalType.BYTES;
      case "DATE":
        return LogicalType.DATE;
      case "TIME":
      case "TIMESTAMP":
      case "DATETIME":
        return LogicalType.TIMESTAMP;
      case "JSON":
        return LogicalType.JSON;
      default:
        return LogicalType.STRING;
    }
  }
}
