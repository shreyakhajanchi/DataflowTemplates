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
package com.google.cloud.teleport.v2.templates.common;

import com.google.cloud.teleport.v2.templates.model.LogicalType;
import com.google.cloud.teleport.v2.templates.model.SinkDialect;
import java.io.Serializable;

/** Interface for mapping source types to {@link LogicalType}. */
public interface TypeMapper extends Serializable {

  /**
   * Maps a source type string to a {@link LogicalType}.
   *
   * @param typeName The type name from the source database (e.g., "VARCHAR", "INT64").
   * @param dialect The dialect of the source database.
   * @return The corresponding {@link LogicalType}.
   */
  LogicalType getLogicalType(String typeName, SinkDialect dialect);
}
