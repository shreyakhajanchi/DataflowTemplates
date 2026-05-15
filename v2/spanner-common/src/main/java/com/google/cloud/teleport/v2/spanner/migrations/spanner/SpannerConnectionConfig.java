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
package com.google.cloud.teleport.v2.spanner.migrations.spanner;

import java.io.Serializable;
import java.util.Objects;

/** Configuration for a Cloud Spanner connection representing the parsed JSON config file. */
public class SpannerConnectionConfig implements Serializable {
  private String projectId;
  private String instanceId;
  private String databaseId;

  public SpannerConnectionConfig() {}

  public SpannerConnectionConfig(String projectId, String instanceId, String databaseId) {
    this.projectId = projectId;
    this.instanceId = instanceId;
    this.databaseId = databaseId;
  }

  public String getProjectId() {
    return projectId;
  }

  public void setProjectId(String projectId) {
    this.projectId = projectId;
  }

  public String getInstanceId() {
    return instanceId;
  }

  public void setInstanceId(String instanceId) {
    this.instanceId = instanceId;
  }

  public String getDatabaseId() {
    return databaseId;
  }

  public void setDatabaseId(String databaseId) {
    this.databaseId = databaseId;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (!(o instanceof SpannerConnectionConfig)) {
      return false;
    }
    SpannerConnectionConfig that = (SpannerConnectionConfig) o;
    return Objects.equals(projectId, that.projectId)
        && Objects.equals(instanceId, that.instanceId)
        && Objects.equals(databaseId, that.databaseId);
  }

  @Override
  public int hashCode() {
    return Objects.hash(projectId, instanceId, databaseId);
  }

  @Override
  public String toString() {
    return "SpannerConnectionConfig{"
        + "projectId='"
        + projectId
        + '\''
        + ", instanceId='"
        + instanceId
        + '\''
        + ", databaseId='"
        + databaseId
        + '\''
        + '}';
  }
}
