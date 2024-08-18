/*
 * Copyright (C) 2024 Google LLC
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
package com.google.cloud.teleport.v2.templates.transform;

import static com.google.cloud.teleport.v2.spanner.migrations.constants.Constants.EVENT_SCHEMA_KEY;
import static com.google.cloud.teleport.v2.spanner.migrations.constants.Constants.MYSQL_SOURCE_TYPE;
import static com.google.cloud.teleport.v2.templates.datastream.DatastreamConstants.EVENT_CHANGE_TYPE_KEY;
import static com.google.cloud.teleport.v2.templates.datastream.DatastreamConstants.EVENT_TABLE_NAME_KEY;

import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.auto.value.AutoValue;
import com.google.cloud.teleport.v2.spanner.ddl.Ddl;
import com.google.cloud.teleport.v2.spanner.exceptions.InvalidTransformationException;
import com.google.cloud.teleport.v2.spanner.migrations.convertors.ChangeEventSessionConvertor;
import com.google.cloud.teleport.v2.spanner.migrations.convertors.ChangeEventToMapConvertor;
import com.google.cloud.teleport.v2.spanner.migrations.exceptions.DroppedTableException;
import com.google.cloud.teleport.v2.spanner.migrations.exceptions.InvalidChangeEventException;
import com.google.cloud.teleport.v2.spanner.migrations.schema.Schema;
import com.google.cloud.teleport.v2.spanner.migrations.transformation.CustomTransformation;
import com.google.cloud.teleport.v2.spanner.migrations.transformation.TransformationContext;
import com.google.cloud.teleport.v2.spanner.migrations.utils.CustomTransformationImplFetcher;
import com.google.cloud.teleport.v2.spanner.utils.ISpannerMigrationTransformer;
import com.google.cloud.teleport.v2.spanner.utils.MigrationTransformationRequest;
import com.google.cloud.teleport.v2.spanner.utils.MigrationTransformationResponse;
import com.google.cloud.teleport.v2.templates.constants.DatastreamToSpannerConstants;
import com.google.cloud.teleport.v2.values.FailsafeElement;
import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;
import javax.annotation.Nullable;
import org.apache.beam.sdk.io.gcp.spanner.SpannerAccessor;
import org.apache.beam.sdk.io.gcp.spanner.SpannerConfig;
import org.apache.beam.sdk.metrics.Counter;
import org.apache.beam.sdk.metrics.Distribution;
import org.apache.beam.sdk.metrics.Metrics;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.values.PCollectionView;
import org.apache.beam.sdk.values.TupleTag;
import org.joda.time.Duration;
import org.joda.time.Instant;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@AutoValue
public abstract class ChangeEventTransformerDoFn
    extends DoFn<FailsafeElement<String, String>, FailsafeElement<String, String>>
    implements Serializable {

  private static final Logger LOG = LoggerFactory.getLogger(ChangeEventTransformerDoFn.class);

  // Jackson Object mapper.
  private transient ObjectMapper mapper;

  private ISpannerMigrationTransformer datastreamToSpannerTransformer;

  // ChangeEventSessionConvertor utility object.
  private ChangeEventSessionConvertor changeEventSessionConvertor;

  /* SpannerAccessor must be transient so that its value is not serialized at runtime. */
  private transient SpannerAccessor spannerAccessor;

  @Nullable
  public abstract Schema schema();

  @Nullable
  public abstract TransformationContext transformationContext();

  public abstract String sourceType();

  @Nullable
  public abstract CustomTransformation customTransformation();

  @Nullable
  public abstract Boolean roundJsonDecimals();

  public abstract PCollectionView<Ddl> ddlView();

  public abstract SpannerConfig spannerConfig();

  private Map<String, String> myMap = new HashMap<>();

  private final Counter processedEvents =
      Metrics.counter(ChangeEventTransformerDoFn.class, "Total events processed");

  private final Counter filteredEvents =
      Metrics.counter(ChangeEventTransformerDoFn.class, "Filtered events");

  private final Counter transformedEvents =
      Metrics.counter(ChangeEventTransformerDoFn.class, "Transformed events");

  private final Counter skippedEvents =
      Metrics.counter(ChangeEventTransformerDoFn.class, "Skipped events");
  private final Counter failedEvents =
      Metrics.counter(ChangeEventTransformerDoFn.class, "Other permanent errors");

  private final Counter customTransformationException =
      Metrics.counter(ChangeEventTransformerDoFn.class, "Custom Transformation Exceptions");

  private final Distribution applyCustomTransformationResponseTimeMetric =
      Metrics.distribution(
          ChangeEventTransformerDoFn.class, "apply_custom_transformation_impl_latency_ms");

  public static ChangeEventTransformerDoFn create(
      Schema schema,
      TransformationContext transformationContext,
      String sourceType,
      CustomTransformation customTransformation,
      Boolean roundJsonDecimals,
      PCollectionView<Ddl> ddlView,
      SpannerConfig spannerConfig) {
    return new AutoValue_ChangeEventTransformerDoFn(
        schema,
        transformationContext,
        sourceType,
        customTransformation,
        roundJsonDecimals,
        ddlView,
        spannerConfig);
  }

  /** Setup function connects to Cloud Spanner. */
  @Setup
  public void setup() {
    mapper = new ObjectMapper();
    mapper.enable(DeserializationFeature.USE_BIG_DECIMAL_FOR_FLOATS);
    datastreamToSpannerTransformer =
        CustomTransformationImplFetcher.getCustomTransformationLogicImpl(customTransformation());
    changeEventSessionConvertor =
        new ChangeEventSessionConvertor(
            schema(), transformationContext(), sourceType(), roundJsonDecimals());
    spannerAccessor = SpannerAccessor.getOrCreate(spannerConfig());
    initializeMap();
  }

  public void initializeMap() {
    myMap.put(
        "projects/443856856628/locations/us-central1/streams/smt-stream-ck-test-70tb-str-2024-06-23-01af-929a",
        "34-172-26-167");
    myMap.put(
        "projects/443856856628/locations/us-central1/streams/smt-stream-ck-test-70tb-str-2024-06-23-01c8-1c94",
        "34-172-26-168");
    myMap.put(
        "projects/443856856628/locations/us-central1/streams/smt-stream-ck-test-70tb-str-2024-06-23-03ab-60d4",
        "34-172-26-169");
    myMap.put(
        "projects/443856856628/locations/us-central1/streams/smt-stream-ck-test-70tb-str-2024-06-23-0490-46d6",
        "34-172-26-170");
    myMap.put(
        "projects/443856856628/locations/us-central1/streams/smt-stream-ck-test-70tb-str-2024-06-23-0543-5ce4",
        "34-172-26-171");
    myMap.put(
        "projects/443856856628/locations/us-central1/streams/smt-stream-ck-test-70tb-str-2024-06-23-065f-c29a",
        "34-172-26-172");
    myMap.put(
        "projects/443856856628/locations/us-central1/streams/smt-stream-ck-test-70tb-str-2024-06-23-072e-23b6",
        "34-172-26-173");
    myMap.put(
        "projects/443856856628/locations/us-central1/streams/smt-stream-ck-test-70tb-str-2024-06-23-07be-bc49",
        "34-172-26-174");
    myMap.put(
        "projects/443856856628/locations/us-central1/streams/smt-stream-ck-test-70tb-str-2024-06-23-09b5-90d4",
        "34-172-26-175");
    myMap.put(
        "projects/443856856628/locations/us-central1/streams/smt-stream-ck-test-70tb-str-2024-06-23-0e61-e25a",
        "34-172-26-176");
    myMap.put(
        "projects/443856856628/locations/us-central1/streams/smt-stream-ck-test-70tb-str-2024-06-23-0f90-729d",
        "34-172-26-177");
    myMap.put(
        "projects/443856856628/locations/us-central1/streams/smt-stream-ck-test-70tb-str-2024-06-23-15fb-425a",
        "34-172-26-178");
    myMap.put(
        "projects/443856856628/locations/us-central1/streams/smt-stream-ck-test-70tb-str-2024-06-23-160f-73f5",
        "34-172-26-179");
    myMap.put(
        "projects/443856856628/locations/us-central1/streams/smt-stream-ck-test-70tb-str-2024-06-23-17b2-83ab",
        "34-172-26-180");
    myMap.put(
        "projects/443856856628/locations/us-central1/streams/smt-stream-ck-test-70tb-str-2024-06-23-199b-9797",
        "34-172-26-181");
    myMap.put(
        "projects/443856856628/locations/us-central1/streams/smt-stream-ck-test-70tb-str-2024-06-23-19f5-052d",
        "34-172-26-182");
    myMap.put(
        "projects/443856856628/locations/us-central1/streams/smt-stream-ck-test-70tb-str-2024-06-23-1c60-f72e",
        "34-172-26-183");
    myMap.put(
        "projects/443856856628/locations/us-central1/streams/smt-stream-ck-test-70tb-str-2024-06-23-1f53-22d9",
        "34-172-26-184");
    myMap.put(
        "projects/443856856628/locations/us-central1/streams/smt-stream-ck-test-70tb-str-2024-06-23-215b-22a2",
        "34-172-26-185");
    myMap.put(
        "projects/443856856628/locations/us-central1/streams/smt-stream-ck-test-70tb-str-2024-06-23-2534-1253",
        "34-172-26-186");
    myMap.put(
        "projects/443856856628/locations/us-central1/streams/smt-stream-ck-test-70tb-str-2024-06-23-2b82-352f",
        "34-172-26-187");
    myMap.put(
        "projects/443856856628/locations/us-central1/streams/smt-stream-ck-test-70tb-str-2024-06-23-3059-2f1f",
        "34-172-26-188");
    myMap.put(
        "projects/443856856628/locations/us-central1/streams/smt-stream-ck-test-70tb-str-2024-06-23-364f-8c12",
        "34-172-26-189");
    myMap.put(
        "projects/443856856628/locations/us-central1/streams/smt-stream-ck-test-70tb-str-2024-06-23-365d-533c",
        "34-172-26-190");
    myMap.put(
        "projects/443856856628/locations/us-central1/streams/smt-stream-ck-test-70tb-str-2024-06-23-3665-45a5",
        "34-172-26-191");
    myMap.put(
        "projects/443856856628/locations/us-central1/streams/smt-stream-ck-test-70tb-str-2024-06-23-3748-673c",
        "34-172-26-192");
    myMap.put(
        "projects/443856856628/locations/us-central1/streams/smt-stream-ck-test-70tb-str-2024-06-23-39dd-f639",
        "34-172-26-193");
    myMap.put(
        "projects/443856856628/locations/us-central1/streams/smt-stream-ck-test-70tb-str-2024-06-23-3abf-3043",
        "34-172-26-194");
    myMap.put(
        "projects/443856856628/locations/us-central1/streams/smt-stream-ck-test-70tb-str-2024-06-23-3b8f-2414",
        "34-172-26-195");
    myMap.put(
        "projects/443856856628/locations/us-central1/streams/smt-stream-ck-test-70tb-str-2024-06-23-3ee7-85d9",
        "34-172-26-196");
    myMap.put(
        "projects/443856856628/locations/us-central1/streams/smt-stream-ck-test-70tb-str-2024-06-23-3f6e-11fa",
        "34-172-26-197");
    myMap.put(
        "projects/443856856628/locations/us-central1/streams/smt-stream-ck-test-70tb-str-2024-06-23-41b0-c224",
        "34-172-26-198");
    myMap.put(
        "projects/443856856628/locations/us-central1/streams/smt-stream-ck-test-70tb-str-2024-06-23-421f-73d8",
        "34-172-26-199");
    myMap.put(
        "projects/443856856628/locations/us-central1/streams/smt-stream-ck-test-70tb-str-2024-06-23-42b3-dea0",
        "34-172-26-200");
    myMap.put(
        "projects/443856856628/locations/us-central1/streams/smt-stream-ck-test-70tb-str-2024-06-23-466d-518b",
        "34-172-26-201");
    myMap.put(
        "projects/443856856628/locations/us-central1/streams/smt-stream-ck-test-70tb-str-2024-06-23-46a5-75df",
        "34-172-26-202");
    myMap.put(
        "projects/443856856628/locations/us-central1/streams/smt-stream-ck-test-70tb-str-2024-06-23-4f92-f026",
        "34-172-26-203");
    myMap.put(
        "projects/443856856628/locations/us-central1/streams/smt-stream-ck-test-70tb-str-2024-06-23-512c-3755",
        "34-172-26-204");
    myMap.put(
        "projects/443856856628/locations/us-central1/streams/smt-stream-ck-test-70tb-str-2024-06-23-524b-75ee",
        "34-172-26-205");
    myMap.put(
        "projects/443856856628/locations/us-central1/streams/smt-stream-ck-test-70tb-str-2024-06-23-5ca6-b707",
        "34-172-26-206");
    myMap.put(
        "projects/443856856628/locations/us-central1/streams/smt-stream-ck-test-70tb-str-2024-06-23-5cc0-84cd",
        "34-172-26-207");
    myMap.put(
        "projects/443856856628/locations/us-central1/streams/smt-stream-ck-test-70tb-str-2024-06-23-608c-8aff",
        "34-172-26-208");
    myMap.put(
        "projects/443856856628/locations/us-central1/streams/smt-stream-ck-test-70tb-str-2024-06-23-60bd-b62e",
        "34-172-26-209");
    myMap.put(
        "projects/443856856628/locations/us-central1/streams/smt-stream-ck-test-70tb-str-2024-06-23-60c2-423c",
        "34-172-26-210");
    myMap.put(
        "projects/443856856628/locations/us-central1/streams/smt-stream-ck-test-70tb-str-2024-06-23-6250-6c82",
        "34-172-26-211");
    myMap.put(
        "projects/443856856628/locations/us-central1/streams/smt-stream-ck-test-70tb-str-2024-06-23-66c9-cbb7",
        "34-172-26-212");
    myMap.put(
        "projects/443856856628/locations/us-central1/streams/smt-stream-ck-test-70tb-str-2024-06-23-6919-6a09",
        "34-172-26-213");
    myMap.put(
        "projects/443856856628/locations/us-central1/streams/smt-stream-ck-test-70tb-str-2024-06-23-69dd-1632",
        "34-172-26-214");
    myMap.put(
        "projects/443856856628/locations/us-central1/streams/smt-stream-ck-test-70tb-str-2024-06-23-6afe-3c83",
        "34-172-26-215");
    myMap.put(
        "projects/443856856628/locations/us-central1/streams/smt-stream-ck-test-70tb-str-2024-06-23-6b02-c585",
        "34-172-26-216");
    myMap.put(
        "projects/443856856628/locations/us-central1/streams/smt-stream-ck-test-70tb-str-2024-06-23-6b04-e3e7",
        "34-172-26-217");
    myMap.put(
        "projects/443856856628/locations/us-central1/streams/smt-stream-ck-test-70tb-str-2024-06-23-6c4e-5acf",
        "34-172-26-218");
    myMap.put(
        "projects/443856856628/locations/us-central1/streams/smt-stream-ck-test-70tb-str-2024-06-23-6c5c-fe95",
        "34-172-26-219");
    myMap.put(
        "projects/443856856628/locations/us-central1/streams/smt-stream-ck-test-70tb-str-2024-06-23-6e2f-6605",
        "34-172-26-220");
    myMap.put(
        "projects/443856856628/locations/us-central1/streams/smt-stream-ck-test-70tb-str-2024-06-23-6f53-fe96",
        "34-172-26-221");
    myMap.put(
        "projects/443856856628/locations/us-central1/streams/smt-stream-ck-test-70tb-str-2024-06-23-7127-9192",
        "34-172-26-222");
    myMap.put(
        "projects/443856856628/locations/us-central1/streams/smt-stream-ck-test-70tb-str-2024-06-23-74e4-37b0",
        "34-172-26-223");
    myMap.put(
        "projects/443856856628/locations/us-central1/streams/smt-stream-ck-test-70tb-str-2024-06-23-7624-82bb",
        "34-172-26-224");
    myMap.put(
        "projects/443856856628/locations/us-central1/streams/smt-stream-ck-test-70tb-str-2024-06-23-7627-cc92",
        "34-172-26-225");
    myMap.put(
        "projects/443856856628/locations/us-central1/streams/smt-stream-ck-test-70tb-str-2024-06-23-77e6-1ee1",
        "34-172-26-226");
    myMap.put(
        "projects/443856856628/locations/us-central1/streams/smt-stream-ck-test-70tb-str-2024-06-23-7824-7973",
        "34-172-26-227");
    myMap.put(
        "projects/443856856628/locations/us-central1/streams/smt-stream-ck-test-70tb-str-2024-06-23-789b-1840",
        "34-172-26-228");
    myMap.put(
        "projects/443856856628/locations/us-central1/streams/smt-stream-ck-test-70tb-str-2024-06-23-7bdf-047d",
        "34-172-26-229");
    myMap.put(
        "projects/443856856628/locations/us-central1/streams/smt-stream-ck-test-70tb-str-2024-06-23-7dd6-f5bd",
        "34-172-26-230");
    myMap.put(
        "projects/443856856628/locations/us-central1/streams/smt-stream-ck-test-70tb-str-2024-06-23-812b-081b",
        "34-172-26-231");
    myMap.put(
        "projects/443856856628/locations/us-central1/streams/smt-stream-ck-test-70tb-str-2024-06-23-86b3-b7b6",
        "34-172-26-232");
    myMap.put(
        "projects/443856856628/locations/us-central1/streams/smt-stream-ck-test-70tb-str-2024-06-23-8a0b-c935",
        "34-172-26-233");
    myMap.put(
        "projects/443856856628/locations/us-central1/streams/smt-stream-ck-test-70tb-str-2024-06-23-8da7-5df9",
        "34-172-26-234");
    myMap.put(
        "projects/443856856628/locations/us-central1/streams/smt-stream-ck-test-70tb-str-2024-06-23-8e4e-ab27",
        "34-172-26-235");
    myMap.put(
        "projects/443856856628/locations/us-central1/streams/smt-stream-ck-test-70tb-str-2024-06-23-8e62-8fb8",
        "34-172-26-236");
    myMap.put(
        "projects/443856856628/locations/us-central1/streams/smt-stream-ck-test-70tb-str-2024-06-23-8ee8-0113",
        "34-172-26-237");
    myMap.put(
        "projects/443856856628/locations/us-central1/streams/smt-stream-ck-test-70tb-str-2024-06-23-8f88-2e6c",
        "34-172-26-238");
    myMap.put(
        "projects/443856856628/locations/us-central1/streams/smt-stream-ck-test-70tb-str-2024-06-23-8ff8-b68d",
        "34-172-26-239");
    myMap.put(
        "projects/443856856628/locations/us-central1/streams/smt-stream-ck-test-70tb-str-2024-06-23-962e-7852",
        "34-172-26-240");
    myMap.put(
        "projects/443856856628/locations/us-central1/streams/smt-stream-ck-test-70tb-str-2024-06-23-964f-106a",
        "34-172-26-241");
    myMap.put(
        "projects/443856856628/locations/us-central1/streams/smt-stream-ck-test-70tb-str-2024-06-23-972d-24b0",
        "34-172-26-242");
    myMap.put(
        "projects/443856856628/locations/us-central1/streams/smt-stream-ck-test-70tb-str-2024-06-23-976f-2a92",
        "34-172-26-243");
    myMap.put(
        "projects/443856856628/locations/us-central1/streams/smt-stream-ck-test-70tb-str-2024-06-23-9ce5-a359",
        "34-172-26-244");
    myMap.put(
        "projects/443856856628/locations/us-central1/streams/smt-stream-ck-test-70tb-str-2024-06-23-a3f7-d72f",
        "34-172-26-245");
    myMap.put(
        "projects/443856856628/locations/us-central1/streams/smt-stream-ck-test-70tb-str-2024-06-23-a4dd-a3bc",
        "34-172-26-246");
    myMap.put(
        "projects/443856856628/locations/us-central1/streams/smt-stream-ck-test-70tb-str-2024-06-23-a51b-73ff",
        "34-172-26-247");
    myMap.put(
        "projects/443856856628/locations/us-central1/streams/smt-stream-ck-test-70tb-str-2024-06-23-a6b3-1b7c",
        "34-172-26-248");
    myMap.put(
        "projects/443856856628/locations/us-central1/streams/smt-stream-ck-test-70tb-str-2024-06-23-a7e6-0164",
        "34-172-26-249");
    myMap.put(
        "projects/443856856628/locations/us-central1/streams/smt-stream-ck-test-70tb-str-2024-06-23-afb6-a20b",
        "34-172-26-250");
    myMap.put(
        "projects/443856856628/locations/us-central1/streams/smt-stream-ck-test-70tb-str-2024-06-23-b04c-47ab",
        "34-172-26-251");
    myMap.put(
        "projects/443856856628/locations/us-central1/streams/smt-stream-ck-test-70tb-str-2024-06-23-b14d-cefb",
        "34-172-26-252");
    myMap.put(
        "projects/443856856628/locations/us-central1/streams/smt-stream-ck-test-70tb-str-2024-06-23-b1ce-4aff",
        "34-172-26-253");
    myMap.put(
        "projects/443856856628/locations/us-central1/streams/smt-stream-ck-test-70tb-str-2024-06-23-b587-65e1",
        "34-172-26-254");
    myMap.put(
        "projects/443856856628/locations/us-central1/streams/smt-stream-ck-test-70tb-str-2024-06-23-b604-69b6",
        "34-172-26-255");
    myMap.put(
        "projects/443856856628/locations/us-central1/streams/smt-stream-ck-test-70tb-str-2024-06-23-b60c-d8f2",
        "34-172-26-256");
    myMap.put(
        "projects/443856856628/locations/us-central1/streams/smt-stream-ck-test-70tb-str-2024-06-23-b7da-af70",
        "34-172-26-257");
    myMap.put(
        "projects/443856856628/locations/us-central1/streams/smt-stream-ck-test-70tb-str-2024-06-23-b927-f5b3",
        "34-172-26-258");
    myMap.put(
        "projects/443856856628/locations/us-central1/streams/smt-stream-ck-test-70tb-str-2024-06-23-bf50-7a76",
        "34-172-26-259");
    myMap.put(
        "projects/443856856628/locations/us-central1/streams/smt-stream-ck-test-70tb-str-2024-06-23-bf62-12c4",
        "34-172-26-260");
    myMap.put(
        "projects/443856856628/locations/us-central1/streams/smt-stream-ck-test-70tb-str-2024-06-23-c1c6-5df5",
        "34-172-26-261");
    myMap.put(
        "projects/443856856628/locations/us-central1/streams/smt-stream-ck-test-70tb-str-2024-06-23-c2b1-dfbd",
        "34-172-26-262");
    myMap.put(
        "projects/443856856628/locations/us-central1/streams/smt-stream-ck-test-70tb-str-2024-06-23-c69b-e5b8",
        "34-172-26-263");
    myMap.put(
        "projects/443856856628/locations/us-central1/streams/smt-stream-ck-test-70tb-str-2024-06-23-c965-b1f5",
        "34-172-26-264");
    myMap.put(
        "projects/443856856628/locations/us-central1/streams/smt-stream-ck-test-70tb-str-2024-06-23-c997-f59d",
        "34-172-26-265");
    myMap.put(
        "projects/443856856628/locations/us-central1/streams/smt-stream-ck-test-70tb-str-2024-06-23-cb09-90d1",
        "34-172-26-266");
    myMap.put(
        "projects/443856856628/locations/us-central1/streams/smt-stream-ck-test-70tb-str-2024-06-23-cc4b-3d47",
        "34-172-26-267");
    myMap.put(
        "projects/443856856628/locations/us-central1/streams/smt-stream-ck-test-70tb-str-2024-06-23-cd04-d3bb",
        "34-172-26-268");
    myMap.put(
        "projects/443856856628/locations/us-central1/streams/smt-stream-ck-test-70tb-str-2024-06-23-d328-45c1",
        "34-172-26-269");
    myMap.put(
        "projects/443856856628/locations/us-central1/streams/smt-stream-ck-test-70tb-str-2024-06-23-d4e5-4d09",
        "34-172-26-270");
    myMap.put(
        "projects/443856856628/locations/us-central1/streams/smt-stream-ck-test-70tb-str-2024-06-23-d545-0eb7",
        "34-172-26-271");
    myMap.put(
        "projects/443856856628/locations/us-central1/streams/smt-stream-ck-test-70tb-str-2024-06-23-d5a6-024f",
        "34-172-26-272");
    myMap.put(
        "projects/443856856628/locations/us-central1/streams/smt-stream-ck-test-70tb-str-2024-06-23-d6d0-9edf",
        "34-172-26-273");
    myMap.put(
        "projects/443856856628/locations/us-central1/streams/smt-stream-ck-test-70tb-str-2024-06-23-d758-523c",
        "34-172-26-274");
    myMap.put(
        "projects/443856856628/locations/us-central1/streams/smt-stream-ck-test-70tb-str-2024-06-23-d908-e6ae",
        "34-172-26-275");
    myMap.put(
        "projects/443856856628/locations/us-central1/streams/smt-stream-ck-test-70tb-str-2024-06-23-d91c-a694",
        "34-172-26-276");
    myMap.put(
        "projects/443856856628/locations/us-central1/streams/smt-stream-ck-test-70tb-str-2024-06-23-dbe3-cbf0",
        "34-172-26-277");
    myMap.put(
        "projects/443856856628/locations/us-central1/streams/smt-stream-ck-test-70tb-str-2024-06-23-dd48-aaa6",
        "34-172-26-278");
    myMap.put(
        "projects/443856856628/locations/us-central1/streams/smt-stream-ck-test-70tb-str-2024-06-23-dec2-e234",
        "34-172-26-279");
    myMap.put(
        "projects/443856856628/locations/us-central1/streams/smt-stream-ck-test-70tb-str-2024-06-23-e008-32dc",
        "34-172-26-280");
    myMap.put(
        "projects/443856856628/locations/us-central1/streams/smt-stream-ck-test-70tb-str-2024-06-23-e0a3-3827",
        "34-172-26-281");
    myMap.put(
        "projects/443856856628/locations/us-central1/streams/smt-stream-ck-test-70tb-str-2024-06-23-e58c-671f",
        "34-172-26-282");
    myMap.put(
        "projects/443856856628/locations/us-central1/streams/smt-stream-ck-test-70tb-str-2024-06-23-e882-92f7",
        "34-172-26-283");
    myMap.put(
        "projects/443856856628/locations/us-central1/streams/smt-stream-ck-test-70tb-str-2024-06-23-ed61-4418",
        "34-172-26-284");
    myMap.put(
        "projects/443856856628/locations/us-central1/streams/smt-stream-ck-test-70tb-str-2024-06-23-eeb0-0a5a",
        "34-172-26-285");
    myMap.put(
        "projects/443856856628/locations/us-central1/streams/smt-stream-ck-test-70tb-str-2024-06-23-ef04-7c44",
        "34-172-26-286");
    myMap.put(
        "projects/443856856628/locations/us-central1/streams/smt-stream-ck-test-70tb-str-2024-06-23-efa2-89dd",
        "34-172-26-287");
    myMap.put(
        "projects/443856856628/locations/us-central1/streams/smt-stream-ck-test-70tb-str-2024-06-23-f13e-9b4d",
        "34-172-26-288");
    myMap.put(
        "projects/443856856628/locations/us-central1/streams/smt-stream-ck-test-70tb-str-2024-06-23-f5b8-dafd",
        "34-172-26-289");
    myMap.put(
        "projects/443856856628/locations/us-central1/streams/smt-stream-ck-test-70tb-str-2024-06-23-f90a-f800",
        "34-172-26-290");
    myMap.put(
        "projects/443856856628/locations/us-central1/streams/smt-stream-ck-test-70tb-str-2024-06-23-f918-b26b",
        "34-172-26-291");
    myMap.put(
        "projects/443856856628/locations/us-central1/streams/smt-stream-ck-test-70tb-str-2024-06-23-fab5-6e42",
        "34-172-26-292");
    myMap.put(
        "projects/443856856628/locations/us-central1/streams/smt-stream-ck-test-70tb-str-2024-06-23-fe9b-447a",
        "34-172-26-293");
  }

  @ProcessElement
  public void processElement(ProcessContext c) {
    FailsafeElement<String, String> msg = c.element();
    processedEvents.inc();
    Ddl ddl = c.sideInput(ddlView());
    try {

      JsonNode changeEvent = mapper.readTree(msg.getOriginalPayload());
      Map<String, Object> sourceRecord =
          ChangeEventToMapConvertor.convertChangeEventToMap(changeEvent);

      if (!schema().isEmpty()) {
        schema().verifyTableInSession(changeEvent.get(EVENT_TABLE_NAME_KEY).asText());
        changeEvent = changeEventSessionConvertor.transformChangeEventViaSessionFile(changeEvent);
      }

      String streamValue = changeEvent.get("_metadata_stream").asText();

      // Find the value after the last "/"
      String lastSegment = streamValue.substring(streamValue.lastIndexOf('/') + 1);

      ((ObjectNode) changeEvent)
          .put(
              "migration_shard_id",
              lastSegment + "_" + changeEvent.get("_metadata_schema").asText());

      changeEvent =
          changeEventSessionConvertor.transformChangeEventData(
              changeEvent, spannerAccessor.getDatabaseClient(), ddl);

      // If custom jar is specified apply custom transformation to the change event
      if (datastreamToSpannerTransformer != null) {
        MigrationTransformationResponse migrationTransformationResponse = null;
        try {
          migrationTransformationResponse =
              getCustomTransformationResponse(changeEvent, sourceRecord);
          if (migrationTransformationResponse.isEventFiltered()) {
            filteredEvents.inc();
            c.output(DatastreamToSpannerConstants.FILTERED_EVENT_TAG, msg.getOriginalPayload());
            return;
          }
          if (migrationTransformationResponse != null
              && migrationTransformationResponse.getResponseRow() != null) {
            changeEvent =
                ChangeEventToMapConvertor.transformChangeEventViaCustomTransformation(
                    changeEvent, migrationTransformationResponse.getResponseRow());
          }
        } catch (Exception e) {
          throw new InvalidTransformationException(e);
        }
      }
      transformedEvents.inc();
      // Adding the original payload to the Failsafe element to ensure that input is not mutated in
      // case of retries.
      c.output(
          DatastreamToSpannerConstants.TRANSFORMED_EVENT_TAG,
          FailsafeElement.of(msg.getOriginalPayload(), changeEvent.toString()));
    } catch (DroppedTableException e) {
      // Errors when table exists in source but was dropped during conversion. We do not output any
      // errors to dlq for this.
      LOG.warn(e.getMessage());
      skippedEvents.inc();
    } catch (InvalidTransformationException e) {
      // Errors that result from the custom JAR during transformation are not retryable.
      outputWithErrorTag(c, msg, e, DatastreamToSpannerConstants.PERMANENT_ERROR_TAG);
      customTransformationException.inc();
    } catch (InvalidChangeEventException e) {
      // Errors that result from invalid change events.
      outputWithErrorTag(c, msg, e, DatastreamToSpannerConstants.PERMANENT_ERROR_TAG);
      skippedEvents.inc();
    } catch (Exception e) {
      // Any other errors are considered severe and not retryable.
      outputWithErrorTag(c, msg, e, DatastreamToSpannerConstants.PERMANENT_ERROR_TAG);
      failedEvents.inc();
    }
  }

  MigrationTransformationResponse getCustomTransformationResponse(
      JsonNode changeEvent, Map<String, Object> sourceRecord)
      throws InvalidTransformationException {
    String shardId = "";
    String tableName = changeEvent.get(EVENT_TABLE_NAME_KEY).asText();

    // Fetch shard id from transformation context.
    if (transformationContext() != null && MYSQL_SOURCE_TYPE.equals(sourceType())) {
      Map<String, String> schemaToShardId = transformationContext().getSchemaToShardId();
      if (schemaToShardId != null && !schemaToShardId.isEmpty()) {
        String schemaName = changeEvent.get(EVENT_SCHEMA_KEY).asText();
        shardId = schemaToShardId.getOrDefault(schemaName, "");
      }
    }
    Instant startTimestamp = Instant.now();
    MigrationTransformationRequest migrationTransformationRequest =
        new MigrationTransformationRequest(
            tableName, sourceRecord, shardId, changeEvent.get(EVENT_CHANGE_TYPE_KEY).asText());
    MigrationTransformationResponse migrationTransformationResponse =
        datastreamToSpannerTransformer.toSpannerRow(migrationTransformationRequest);
    Instant endTimestamp = Instant.now();
    applyCustomTransformationResponseTimeMetric.update(
        new Duration(startTimestamp, endTimestamp).getMillis());
    return migrationTransformationResponse;
  }

  void outputWithErrorTag(
      ProcessContext c,
      FailsafeElement<String, String> changeEvent,
      Exception e,
      TupleTag<FailsafeElement<String, String>> errorTag) {
    // Making a copy, as the input must not be mutated.
    FailsafeElement<String, String> output = FailsafeElement.of(changeEvent);
    output.setErrorMessage(e.getMessage());
    c.output(errorTag, output);
  }

  public void setMapper(ObjectMapper mapper) {
    this.mapper = mapper;
  }

  public void setChangeEventSessionConvertor(
      ChangeEventSessionConvertor changeEventSessionConvertor) {
    this.changeEventSessionConvertor = changeEventSessionConvertor;
  }

  public void setDatastreamToSpannerTransformer(
      ISpannerMigrationTransformer datastreamToSpannerTransformer) {
    this.datastreamToSpannerTransformer = datastreamToSpannerTransformer;
  }

  public void setSpannerAccessor(SpannerAccessor spannerAccessor) {
    this.spannerAccessor = spannerAccessor;
  }
}
