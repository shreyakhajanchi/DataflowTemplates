/*
 * Copyright (C) 2023 Google LLC
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
package com.google.cloud.teleport.plugin;

import java.io.BufferedReader;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import org.slf4j.Logger;

/** Utility methods that can be used on the plugin. */
public final class TemplatePluginUtils {

  /**
   * Redirect an InputStream to a logger for testing and debugging purposes.
   *
   * @param inputStream The InputStream to redirect.
   * @param log The logger to redirect the InputStream to.
   */
  public static void redirectLinesLog(
      InputStream inputStream, Logger log, StringBuilder cloudBuildLogs) {
    new Thread(
            () -> {
              try (InputStreamReader isr =
                      new InputStreamReader(inputStream, StandardCharsets.UTF_8);
                  BufferedReader bis = new BufferedReader(isr)) {

                String line;
                while ((line = bis.readLine()) != null) {
                  log.info(line);
                  if (cloudBuildLogs != null && line.contains("Logs are available at")) {
                    cloudBuildLogs.append(line);
                  }
                }
              } catch (Exception e) {
                log.error("Error redirecting stream", e);
              }
            })
        .start();
  }
}
