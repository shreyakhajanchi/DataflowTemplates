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

import com.google.cloud.teleport.v2.templates.DataGeneratorOptions;
import com.google.cloud.teleport.v2.templates.model.DataGeneratorSchema;
import com.google.cloud.teleport.v2.templates.model.DataGeneratorTable;
import org.apache.beam.sdk.state.StateSpec;
import org.apache.beam.sdk.state.StateSpecs;
import org.apache.beam.sdk.state.Timer;
import org.apache.beam.sdk.state.TimerSpec;
import org.apache.beam.sdk.state.TimerSpecs;
import org.apache.beam.sdk.state.ValueState;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionView;
import org.joda.time.Instant;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A {@link PTransform} that generates ticks based on the total QPS of root tables in the schema.
 */
public class GenerateTicks extends PTransform<PCollection<byte[]>, PCollection<Long>> {

  private final DataGeneratorOptions options;
  private final PCollectionView<DataGeneratorSchema> schemaView;

  public GenerateTicks(
      DataGeneratorOptions options, PCollectionView<DataGeneratorSchema> schemaView) {
    this.options = options;
    this.schemaView = schemaView;
  }

  @Override
  public PCollection<Long> expand(PCollection<byte[]> input) {
    int qpsPerPartition =
        options.getQpsPerPartition() != null ? options.getQpsPerPartition() : 1000;
    return input
        .apply(
            "ShardTicks",
            ParDo.of(new ShardTicksDoFn(schemaView, qpsPerPartition)).withSideInputs(schemaView))
        .apply(
            "GenerateTicksFn",
            ParDo.of(new GenerateTicksFn(schemaView, qpsPerPartition)).withSideInputs(schemaView));
  }

  static class ShardTicksDoFn extends DoFn<byte[], KV<Integer, byte[]>> {
    private final PCollectionView<DataGeneratorSchema> schemaView;
    private final int qpsPerPartition;

    ShardTicksDoFn(PCollectionView<DataGeneratorSchema> schemaView, int qpsPerPartition) {
      this.schemaView = schemaView;
      this.qpsPerPartition = qpsPerPartition;
    }

    @ProcessElement
    public void process(ProcessContext c) {
      DataGeneratorSchema schema = c.sideInput(schemaView);
      int totalQps = 0;
      for (DataGeneratorTable table : schema.tables().values()) {
        if (table.isRoot()) {
          totalQps += table.qps();
        }
      }

      int numPartitions = (int) Math.ceil((double) totalQps / qpsPerPartition);
      if (numPartitions < 1) {
        numPartitions = 1;
      }

      LOG.info(
          "Total QPS: {}, QPS per Partition: {}, Num Partitions: {}",
          totalQps,
          qpsPerPartition,
          numPartitions);
      if (totalQps > 0 && numPartitions > 0 && totalQps / numPartitions > 1000) {
        LOG.warn(
            "QPS per partition ({}) is greater than 1000. This may exceed the Timer firing resolution (1ms) and result in lower than expected throughput. Consider increasing partitions or lowering QPS.",
            totalQps / numPartitions);
      }

      for (int i = 0; i < numPartitions; i++) {
        c.output(KV.of(i, c.element()));
      }
    }

    private static final Logger LOG = LoggerFactory.getLogger(ShardTicksDoFn.class);
  }

  static class GenerateTicksFn extends DoFn<KV<Integer, byte[]>, Long> {
    private final PCollectionView<DataGeneratorSchema> schemaView;
    private final int qpsPerPartition;

    @TimerId("tickTimer")
    private final TimerSpec tickTimer =
        TimerSpecs.timer(org.apache.beam.sdk.state.TimeDomain.PROCESSING_TIME);

    @StateId("shardQps")
    private final StateSpec<ValueState<Integer>> shardQpsSpec = StateSpecs.value();

    @StateId("nextTick")
    private final StateSpec<ValueState<Long>> nextTickSpec = StateSpecs.value();

    GenerateTicksFn(PCollectionView<DataGeneratorSchema> schemaView, int qpsPerPartition) {
      this.schemaView = schemaView;
      this.qpsPerPartition = qpsPerPartition;
    }

    private static final Logger LOG = LoggerFactory.getLogger(GenerateTicksFn.class);

    @ProcessElement
    public void processElement(
        ProcessContext c,
        @TimerId("tickTimer") Timer timer,
        @StateId("nextTick") ValueState<Long> nextTickState,
        @StateId("shardQps") ValueState<Integer> shardQpsState) {

      DataGeneratorSchema schema = c.sideInput(schemaView);
      int totalQps = 0;
      for (DataGeneratorTable table : schema.tables().values()) {
        if (table.isRoot()) {
          totalQps += table.qps();
        }
      }

      if (totalQps <= 0) {
        LOG.warn("Total QPS is 0 or less. No ticks will be generated.");
        return;
      }

      int numPartitions = (int) Math.ceil((double) totalQps / qpsPerPartition);
      if (numPartitions < 1) {
        numPartitions = 1;
      }

      // Distribute QPS unevenly if needed, but for simplicity:
      // We can just calculate "My QPS" based on total/numPartitions?
      // Or better: Distribute remainder.
      // e.g. 1005 QPS, 1000 per partition -> 2 partitions.
      // Partition 0, 1.
      // We want ~502 and ~503? Or 1000 and 5?
      // "NumPartitions" logic in previous step created N shards.
      // To be consistent, we should just divide TotalQPS / NumPartitions.
      // And maybe add 1 to the first (TotalQPS % NumPartitions) shards.

      int shardId = c.element().getKey();
      int baseQps = totalQps / numPartitions;
      int remainder = totalQps % numPartitions;

      int myQps = baseQps + (shardId < remainder ? 1 : 0);

      if (myQps <= 0) {
        return;
      }

      // Persist QPS for OnTimer
      shardQpsState.write(myQps);

      // Calculate interval in milliseconds
      long intervalMs = 1000 / myQps;
      if (intervalMs == 0) {
        intervalMs = 1; // Minimum 1ms interval
      }

      Instant now = Instant.now();
      timer.set(now);
      nextTickState.write(now.getMillis() + intervalMs);

      LOG.info(
          "Started ticker for shard {} with QPS: {}, Interval: {}ms", shardId, myQps, intervalMs);
    }

    @OnTimer("tickTimer")
    public void onTick(
        OnTimerContext c,
        @TimerId("tickTimer") Timer timer,
        @StateId("nextTick") ValueState<Long> nextTickState,
        @StateId("shardQps") ValueState<Integer> totalQpsState) {

      Long nextTick = nextTickState.read();
      Integer qps = totalQpsState.read();

      if (nextTick == null || qps == null || qps <= 0) {
        return;
      }

      c.output(nextTick);

      long intervalMs = 1000 / qps;
      if (intervalMs == 0) {
        intervalMs = 1;
      }

      Instant next = Instant.ofEpochMilli(nextTick + intervalMs);
      timer.set(next);
      nextTickState.write(next.getMillis());
    }
  }
}
