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
import java.util.List;
import javax.annotation.Nullable;
import org.apache.beam.sdk.schemas.AutoValueSchema;
import org.apache.beam.sdk.schemas.annotations.DefaultSchema;
import org.apache.beam.sdk.schemas.annotations.SchemaFieldName;

/** Represents a table in the data generator schema. */
@AutoValue
@DefaultSchema(AutoValueSchema.class)
public abstract class DataGeneratorTable {

  /** The name of the table. */
  public abstract String getName();

  /** The columns of the table. */
  // TODO(khajanchi): Consider using a map of columns instead of list
  public abstract List<DataGeneratorColumn> getColumns();

  /** The primary key column names. */
  public abstract List<String> getPrimaryKeys();

  /** The name of the table this table is interleaved in, if any (Spanner specific). */
  @Nullable
  public abstract String getInterleavedInTable();

  /** Foreign keys defined on this table. */
  public abstract List<DataGeneratorForeignKey> getForeignKeys();

  /** Unique keys/indexes defined on this table. */
  public abstract List<DataGeneratorUniqueKey> getUniqueKeys();

  /** Whether this table is a root table (not interleaved/child). */
  @Nullable
  @SchemaFieldName("isRoot")
  public abstract Boolean getRoot();

  /** The QPS for inserts. */
  @Nullable
  public abstract Integer getInsertQps();

  /** The QPS for updates. */
  @Nullable
  public abstract Integer getUpdateQps();

  /** The QPS for deletes. */
  @Nullable
  public abstract Integer getDeleteQps();

  /** The number of records to generate for this table for each record of the parent table. */
  @Nullable
  public abstract Double getRecordsPerTick();

  /** The name of the parent table that drives generation for this table (if any). */
  @Nullable
  public abstract String getGeneratorParent();

  /** The names of the tables that are children of this table in the generation hierarchy. */
  @Nullable
  public abstract List<String> getChildTables();

  public abstract Builder toBuilder();

  public static Builder builder() {
    return new AutoValue_DataGeneratorTable.Builder();
  }

  @AutoValue.Builder
  public abstract static class Builder {
    public abstract Builder name(String name);

    public Builder setName(String name) {
      return name(name);
    }

    public abstract Builder columns(List<DataGeneratorColumn> columns);

    public Builder setColumns(List<DataGeneratorColumn> columns) {
      return columns(columns);
    }

    public abstract Builder primaryKeys(List<String> primaryKeys);

    public Builder setPrimaryKeys(List<String> primaryKeys) {
      return primaryKeys(primaryKeys);
    }

    public abstract Builder interleavedInTable(@Nullable String interleavedInTable);

    public Builder setInterleavedInTable(@Nullable String interleavedInTable) {
      return interleavedInTable(interleavedInTable);
    }

    public abstract Builder foreignKeys(List<DataGeneratorForeignKey> foreignKeys);

    public Builder setForeignKeys(List<DataGeneratorForeignKey> foreignKeys) {
      return foreignKeys(foreignKeys);
    }

    public abstract Builder uniqueKeys(List<DataGeneratorUniqueKey> uniqueKeys);

    public Builder setUniqueKeys(List<DataGeneratorUniqueKey> uniqueKeys) {
      return uniqueKeys(uniqueKeys);
    }

    public abstract Builder root(@Nullable Boolean root);

    public Builder setRoot(@Nullable Boolean root) {
      return root(root);
    }

    public abstract Builder insertQps(@Nullable Integer insertQps);

    public Builder setInsertQps(@Nullable Integer insertQps) {
      return insertQps(insertQps);
    }

    public abstract Builder updateQps(@Nullable Integer updateQps);

    public Builder setUpdateQps(@Nullable Integer updateQps) {
      return updateQps(updateQps);
    }

    public abstract Builder deleteQps(@Nullable Integer deleteQps);

    public Builder setDeleteQps(@Nullable Integer deleteQps) {
      return deleteQps(deleteQps);
    }

    public abstract Builder recordsPerTick(@Nullable Double recordsPerTick);

    public Builder setRecordsPerTick(@Nullable Double recordsPerTick) {
      return recordsPerTick(recordsPerTick);
    }

    public abstract Builder generatorParent(@Nullable String generatorParent);

    public Builder setGeneratorParent(@Nullable String generatorParent) {
      return generatorParent(generatorParent);
    }

    public abstract Builder childTables(List<String> childTables);

    public Builder setChildTables(List<String> childTables) {
      return childTables(childTables);
    }

    public abstract DataGeneratorTable build();
  }
}
