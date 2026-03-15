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
package com.google.cloud.teleport.v2.templates.transforms;

import com.github.javafaker.Faker;
import com.google.cloud.teleport.v2.templates.model.DataGeneratorColumn;
import com.google.cloud.teleport.v2.templates.model.DataGeneratorTable;
import com.google.cloud.teleport.v2.templates.utils.DataGeneratorUtils;
import java.util.List;
import java.util.Random;
import java.util.stream.Collectors;
import org.apache.beam.sdk.coders.KvCoder;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.Row;

/**
 * A {@link PTransform} that generates Primary Key values for the selected table. It uses {@link
 * Faker} to generate realistic fake data based on logical types.
 */
public class GeneratePrimaryKey
    extends PTransform<PCollection<DataGeneratorTable>, PCollection<KV<String, Row>>> {

  @Override
  public PCollection<KV<String, Row>> expand(PCollection<DataGeneratorTable> input) {
    return input
        .apply("GeneratePrimaryKey", ParDo.of(new GeneratePrimaryKeyFn()))
        .setCoder(
            KvCoder.of(
                org.apache.beam.sdk.coders.StringUtf8Coder.of(),
                org.apache.beam.sdk.coders.SerializableCoder.of(Row.class)));
  }

  static class GeneratePrimaryKeyFn extends DoFn<DataGeneratorTable, KV<String, Row>> {
    private transient Faker faker;
    private transient Random random;

    @Setup
    public void setup() {
      faker = new Faker();
      random = new Random();
    }

    @ProcessElement
    public void processElement(
        @Element DataGeneratorTable table, OutputReceiver<KV<String, Row>> out) {
      List<DataGeneratorColumn> pkColumns =
          table.columns().stream()
              .filter(DataGeneratorColumn::isPrimaryKey)
              .collect(Collectors.toList());

      if (pkColumns.isEmpty()) {
        // Fallback: If no PK, maybe return empty Row or log warning?
        // Ideally every table has a PK in this generator.
        return;
      }

      Schema schema = buildSchema(pkColumns);
      Row.Builder rowBuilder = Row.withSchema(schema);

      for (DataGeneratorColumn column : pkColumns) {
        rowBuilder.addValue(generateValue(column));
      }

      out.output(KV.of(table.name(), rowBuilder.build()));
    }

    private Schema buildSchema(List<DataGeneratorColumn> columns) {
      Schema.Builder builder = Schema.builder();
      for (DataGeneratorColumn col : columns) {
        builder.addField(
            Schema.Field.of(col.name(), DataGeneratorUtils.mapToBeamFieldType(col.logicalType())));
      }
      return builder.build();
    }

    // Removed mapToFieldType as it is now in DataGeneratorUtils

    private Object generateValue(DataGeneratorColumn column) {
      return DataGeneratorUtils.generateValue(column, faker);
    }
  }
}
