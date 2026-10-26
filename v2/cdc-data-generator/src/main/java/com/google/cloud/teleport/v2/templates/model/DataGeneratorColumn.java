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

import com.google.auto.value.AutoValue;
import javax.annotation.Nullable;
import org.apache.beam.sdk.schemas.AutoValueSchema;
import org.apache.beam.sdk.schemas.annotations.DefaultSchema;
import org.apache.beam.sdk.schemas.annotations.SchemaFieldName;

/** Represents a column in the data generator schema. */
@AutoValue
@DefaultSchema(AutoValueSchema.class)
public abstract class DataGeneratorColumn {

  /** The name of the column. */
  public abstract String getName();

  /** The logical type of the column for data generation. */
  public abstract LogicalType getLogicalType();

  /** Whether the column is nullable. */
  @SchemaFieldName("isNullable")
  public abstract boolean isNullable();

  /** Whether the column is a primary key. */
  @SchemaFieldName("isPrimaryKey")
  public abstract boolean isPrimaryKey();

  /** Whether the column is skipped. */
  @SchemaFieldName("isSkipped")
  public abstract boolean isSkipped();

  /** Whether the column is a generated column. */
  @SchemaFieldName("isGenerated")
  public abstract boolean isGenerated();

  /** The size/length of the column (e.g., for strings or bytes). */
  @Nullable
  public abstract Long getSize();

  /** The precision of the column (for numeric types). */
  @Nullable
  public abstract Integer getPrecision();

  /** The scale of the column (for numeric types). */
  @Nullable
  public abstract Integer getScale();

  /** The custom generator for this column (e.g., Faker expression). */
  @Nullable
  public abstract Object getFakerExpression();

  public static Builder builder() {
    return new AutoValue_DataGeneratorColumn.Builder().setSkipped(false).setPrimaryKey(false);
  }

  public abstract Builder toBuilder();

  @AutoValue.Builder
  public abstract static class Builder {
    public abstract Builder name(String name);

    public Builder setName(String name) {
      return name(name);
    }

    public abstract Builder logicalType(LogicalType logicalType);

    public Builder setLogicalType(LogicalType logicalType) {
      return logicalType(logicalType);
    }

    public abstract Builder nullable(boolean nullable);

    public Builder setNullable(boolean nullable) {
      return nullable(nullable);
    }

    public abstract Builder skipped(boolean skipped);

    public Builder setSkipped(boolean skipped) {
      return skipped(skipped);
    }

    public abstract Builder generated(boolean generated);

    public Builder setGenerated(boolean generated) {
      return generated(generated);
    }

    public abstract Builder size(Long size);

    public Builder setSize(Long size) {
      return size(size);
    }

    public abstract Builder precision(Integer precision);

    public Builder setPrecision(Integer precision) {
      return precision(precision);
    }

    public abstract Builder scale(Integer scale);

    public Builder setScale(Integer scale) {
      return scale(scale);
    }

    public abstract Builder fakerExpression(Object fakerExpression);

    public Builder setFakerExpression(Object fakerExpression) {
      return fakerExpression(fakerExpression);
    }

    public abstract Builder primaryKey(boolean primaryKey);

    public Builder setPrimaryKey(boolean primaryKey) {
      return primaryKey(primaryKey);
    }

    public abstract DataGeneratorColumn build();
  }
}
