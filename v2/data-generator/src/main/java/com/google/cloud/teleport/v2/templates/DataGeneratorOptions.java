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
package com.google.cloud.teleport.v2.templates;

import com.google.cloud.teleport.metadata.TemplateParameter;
import com.google.cloud.teleport.v2.options.CommonTemplateOptions;
import org.apache.beam.sdk.options.Validation.Required;

public interface DataGeneratorOptions extends CommonTemplateOptions {

  @TemplateParameter.Enum(
      order = 1,
      enumOptions = {
        @TemplateParameter.TemplateEnumOption("SPANNER"),
        @TemplateParameter.TemplateEnumOption("MYSQL")
      },
      description = "Type of sink to generate data for",
      helpText = "The type of sink to generate data for. Supported values: SPANNER, MYSQL.")
  @Required
  SinkType getSinkType();

  void setSinkType(SinkType value);

  @TemplateParameter.GcsReadFile(
      order = 2,
      optional = false,
      description = "Sink Options JSON File Path",
      helpText =
          "GCS Path to a file containing JSON Options for the sink. For Spanner: {\"projectId\": \"...\", \"instanceId\": \"...\", \"databaseId\": \"...\"}. "
              + "For MySQL: {\"driverClassName\": \"...\", \"connectionUrl\": \"...\", \"username\": \"...\", \"password\": \"...\"} or {\"shardFilePath\": \"gs://...\"}",
      example = "gs://your-bucket/path/to/sink_options.json")
  String getSinkOptions();

  void setSinkOptions(String value);

  enum SinkType {
    SPANNER,
    MYSQL
  }
}
