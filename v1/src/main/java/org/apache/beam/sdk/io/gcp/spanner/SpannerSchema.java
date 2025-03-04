/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.beam.sdk.io.gcp.spanner;

import com.google.auto.value.AutoValue;
import com.google.cloud.spanner.Dialect;
import com.google.cloud.spanner.Type;
import java.io.Serializable;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.annotations.VisibleForTesting;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.ImmutableList;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.ImmutableListMultimap;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.ImmutableMap;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.ImmutableTable;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.Maps;

/** Encapsulates Cloud Spanner Schema. */
@AutoValue
@SuppressWarnings({
  "nullness" // TODO(https://github.com/apache/beam/issues/20497)
})
abstract class SpannerSchema implements Serializable {
  abstract ImmutableList<String> tables();

  abstract Dialect dialect();

  abstract ImmutableListMultimap<String, Column> columns();

  abstract ImmutableListMultimap<String, KeyPart> keyParts();

  abstract ImmutableTable<String, String, Long> cellsMutatedPerColumn();

  abstract ImmutableMap<String, Long> cellsMutatedPerRow();

  public static Builder builder() {
    return builder(Dialect.GOOGLE_STANDARD_SQL);
  }

  public static Builder builder(Dialect dialect) {
    return new AutoValue_SpannerSchema.Builder().setDialect(dialect);
  }

  /** Builder for {@link SpannerSchema}. */
  @AutoValue.Builder
  abstract static class Builder {
    abstract Builder setTables(ImmutableList<String> tablesBuilder);

    abstract Builder setDialect(Dialect dialect);

    abstract Dialect dialect();

    abstract ImmutableListMultimap.Builder<String, Column> columnsBuilder();

    abstract ImmutableListMultimap.Builder<String, KeyPart> keyPartsBuilder();

    abstract ImmutableTable.Builder<String, String, Long> cellsMutatedPerColumnBuilder();

    abstract ImmutableMap.Builder<String, Long> cellsMutatedPerRowBuilder();

    abstract ImmutableListMultimap<String, Column> columns();

    abstract ImmutableTable<String, String, Long> cellsMutatedPerColumn();

    @VisibleForTesting
    public Builder addColumn(String table, String name, String type) {
      return addColumn(table, name, type, 1L);
    }

    public Builder addColumn(String table, String name, String type, long cellsMutated) {
      String tableLower = table.toLowerCase();
      String nameLower = name.toLowerCase();

      columnsBuilder().put(tableLower, Column.create(nameLower, type, dialect()));
      cellsMutatedPerColumnBuilder().put(tableLower, nameLower, cellsMutated);
      return this;
    }

    public Builder addKeyPart(String table, String column, boolean desc) {
      keyPartsBuilder().put(table.toLowerCase(), KeyPart.create(column.toLowerCase(), desc));
      return this;
    }

    abstract SpannerSchema autoBuild();

    public final SpannerSchema build() {
      // precompute the number of cells that are mutated for operations affecting
      // an entire row such as a single key delete.
      cellsMutatedPerRowBuilder()
          .putAll(
              Maps.transformValues(
                  cellsMutatedPerColumn().rowMap(),
                  entry -> entry.values().stream().mapToLong(Long::longValue).sum()));

      setTables(ImmutableList.copyOf(columns().keySet()));

      return autoBuild();
    }
  }

  public List<String> getTables() {
    return tables();
  }

  public List<Column> getColumns(String table) {
    return columns().get(table.toLowerCase());
  }

  public List<KeyPart> getKeyParts(String table) {
    return keyParts().get(table.toLowerCase());
  }

  /** Return the total number of cells affected when the specified column is mutated. */
  public long getCellsMutatedPerColumn(String table, String column) {
    return cellsMutatedPerColumn().row(table.toLowerCase()).getOrDefault(column.toLowerCase(), 1L);
  }

  /** Return the total number of cells affected with the given row is deleted. */
  public long getCellsMutatedPerRow(String table) {
    return cellsMutatedPerRow().getOrDefault(table.toLowerCase(), 1L);
  }

  @AutoValue
  abstract static class KeyPart implements Serializable {
    static KeyPart create(String field, boolean desc) {
      return new AutoValue_SpannerSchema_KeyPart(field, desc);
    }

    abstract String getField();

    abstract boolean isDesc();
  }

  @AutoValue
  abstract static class Column implements Serializable {

    private static final Pattern EMBEDDING_VECTOR_PATTERN =
        Pattern.compile(
            "^ARRAY<([a-zA-Z0-9]+)>\\(vector_length=>\\d+\\)$", Pattern.CASE_INSENSITIVE);

    private static final Pattern PG_EMBEDDING_VECTOR_PATTERN =
        Pattern.compile("^(\\D+)\\[\\]\\svector\\slength\\s\\d+$", Pattern.CASE_INSENSITIVE);


    static Column create(String name, Type type) {
      return new AutoValue_SpannerSchema_Column(name, type);
    }

    static Column create(String name, String spannerType, Dialect dialect) {
      return create(name, parseSpannerType(spannerType, dialect));
    }

    public abstract String getName();

    public abstract Type getType();

    private static Type parseSpannerType(String spannerType, Dialect dialect) {
      String originalSpannerType = spannerType;
      spannerType = spannerType.toUpperCase();
      switch (dialect) {
        case GOOGLE_STANDARD_SQL:
          if ("BOOL".equals(spannerType)) {
            return Type.bool();
          }
          if ("INT64".equals(spannerType)) {
            return Type.int64();
          }
          if ("FLOAT32".equals(spannerType)) {
            return Type.float32();
          }
          if ("FLOAT64".equals(spannerType)) {
            return Type.float64();
          }
          if (spannerType.startsWith("STRING")) {
            return Type.string();
          }
          // TODO(b/394493438): Return uuid type once google-cloud-spanner supports UUID type
          if ("UUID".equals(spannerType)) {
            return Type.string();
          }
          if (spannerType.startsWith("BYTES")) {
            return Type.bytes();
          }
          if ("TOKENLIST".equals(spannerType)) {
            return Type.bytes();
          }
          if ("TIMESTAMP".equals(spannerType)) {
            return Type.timestamp();
          }
          if ("DATE".equals(spannerType)) {
            return Type.date();
          }
          if ("NUMERIC".equals(spannerType)) {
            return Type.numeric();
          }
          if ("JSON".equals(spannerType)) {
            return Type.json();
          }
          if(spannerType.startsWith("PROTO")) {
            // Substring "PROTO<xxx>"
            String protoTypeFqn = originalSpannerType
                .substring(6, originalSpannerType.length() - 1);
            return Type.proto(protoTypeFqn);
          }
          if(spannerType.startsWith("ENUM")) {
            // Substring "ENUM<xxx>"
            String enumTypeFqn = originalSpannerType
                .substring(5, originalSpannerType.length() - 1);
            return Type.protoEnum(enumTypeFqn);
          }
          if (spannerType.startsWith("ARRAY")) {
            // Substring "ARRAY<xxx>" or substring "ARRAY<xxx>(vector_length=>yyy)"

            String arrayElementType;
            // Handle vector_length annotation
            Matcher m = EMBEDDING_VECTOR_PATTERN.matcher(spannerType);
            if (m.find()) {
              arrayElementType = m.group(1);
            }
            else {
              arrayElementType = spannerType.substring(6, spannerType.length() - 1);
            }
            Type itemType = parseSpannerType(arrayElementType, dialect);
            return Type.array(itemType);
          }
          throw new IllegalArgumentException("Unknown spanner type " + spannerType);
        case POSTGRESQL:
          // Handle vector_length annotation
          Matcher m = PG_EMBEDDING_VECTOR_PATTERN.matcher(spannerType);
          if (m.find()) {
            // Substring "xxx[] vector length yyy"
            String arrayElementType = m.group(1);
            Type itemType = parseSpannerType(arrayElementType, dialect);
            return Type.array(itemType);
          }
          if (spannerType.endsWith("[]")) {
            // Substring "xxx[]"
            // Must check array type first
            String spannerArrayType = spannerType.substring(0, spannerType.length() - 2);
            Type itemType = parseSpannerType(spannerArrayType, dialect);
            return Type.array(itemType);
          }
          if ("BOOLEAN".equals(spannerType)) {
            return Type.bool();
          }
          if ("BIGINT".equals(spannerType)) {
            return Type.int64();
          }
          if ("REAL".equals(spannerType)) {
            return Type.float32();
          }
          if ("DOUBLE PRECISION".equals(spannerType)) {
            return Type.float64();
          }
          if (spannerType.startsWith("CHARACTER VARYING") || "TEXT".equals(spannerType)) {
            return Type.string();
          }
          if ("BYTEA".equals(spannerType)) {
            return Type.bytes();
          }
          if ("TIMESTAMP WITH TIME ZONE".equals(spannerType)) {
            return Type.timestamp();
          }
          if ("DATE".equals(spannerType)) {
            return Type.date();
          }
          if (spannerType.startsWith("NUMERIC")) {
            return Type.pgNumeric();
          }
          if (spannerType.startsWith("JSONB")) {
            return Type.pgJsonb();
          }
          if ("SPANNER.COMMIT_TIMESTAMP".equals(spannerType)) {
            return Type.timestamp();
          }
          // TODO(b/394493438): Return uuid type once google-cloud-spanner supports UUID type
          if ("UUID".equals(spannerType)) {
            return Type.string();
          }
          if ("SPANNER.TOKENLIST".equals(spannerType)) {
            return Type.bytes();
          }
          throw new IllegalArgumentException("Unknown spanner type " + spannerType);
        default:
          throw new IllegalArgumentException("Unrecognized dialect: " + dialect.name());
      }
    }
  }
}
