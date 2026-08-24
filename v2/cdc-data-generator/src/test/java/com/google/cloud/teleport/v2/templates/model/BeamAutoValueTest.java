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
package com.google.cloud.teleport.v2.templates.model;

import java.util.Collections;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.schemas.SchemaRegistry;
import org.apache.beam.sdk.values.Row;

public class BeamAutoValueTest {
  public static void main(String[] args) throws Exception {
    try {
      SchemaRegistry registry = SchemaRegistry.createDefault();
      Schema schema = registry.getSchema(DataGeneratorSchema.class);
      System.out.println("Got schema for DataGeneratorSchema: " + schema);

      Row row = Row.withSchema(schema).addValue(Collections.emptyMap()).build();
      System.out.println("Created row: " + row);

      DataGeneratorSchema out = registry.getFromRowFunction(DataGeneratorSchema.class).apply(row);
      System.out.println("Out: " + out);
    } catch (Exception e) {
      e.printStackTrace();
    }
  }
}
