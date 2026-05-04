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

import static org.junit.Assert.assertEquals;

import com.google.cloud.teleport.v2.templates.DataGeneratorOptions.SinkType;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.junit.Test;

public class DataGeneratorTest {

  @Test
  public void testOptionsParsing() {
    String[] args = {"--sinkType=SPANNER", "--sinkOptions={\"projectId\": \"p\"}"};

    DataGeneratorOptions options =
        PipelineOptionsFactory.fromArgs(args).as(DataGeneratorOptions.class);

    assertEquals(SinkType.SPANNER, options.getSinkType());
    assertEquals("{\"projectId\": \"p\"}", options.getSinkOptions());
  }
}
