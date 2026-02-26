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
package com.cdc.generator;

import java.io.Serializable;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.coders.DefaultCoder;
import org.apache.beam.sdk.coders.KvCoder;
import org.apache.beam.sdk.coders.SerializableCoder;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.io.GenerateSequence;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.state.BagState;
import org.apache.beam.sdk.state.StateSpec;
import org.apache.beam.sdk.state.StateSpecs;
import org.apache.beam.sdk.state.TimeDomain;
import org.apache.beam.sdk.state.Timer;
import org.apache.beam.sdk.state.TimerSpec;
import org.apache.beam.sdk.state.TimerSpecs;
import org.apache.beam.sdk.state.ValueState;
import org.apache.beam.sdk.transforms.*;
import org.apache.beam.sdk.transforms.GroupIntoBatches;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.TypeDescriptors;
import org.joda.time.Duration;
import org.joda.time.Instant;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.beam.sdk.transforms.DoFn.Element;
import org.apache.beam.sdk.transforms.DoFn.OnTimer;
import org.apache.beam.sdk.transforms.DoFn.ProcessElement;
import org.apache.beam.sdk.transforms.DoFn.Setup;
import org.apache.beam.sdk.transforms.DoFn.StateId;
import org.apache.beam.sdk.transforms.DoFn.Teardown;
import org.apache.beam.sdk.transforms.DoFn.TimerId;
import org.apache.beam.sdk.transforms.DoFn.OutputReceiver;
import org.apache.beam.sdk.state.MapState;
import java.util.TreeMap;
import java.util.List;
import java.util.ArrayList;
import org.apache.beam.sdk.metrics.Counter;
import org.apache.beam.sdk.metrics.Metrics;

public class CdcLifecyclePoc {

  private static final Logger LOG = LoggerFactory.getLogger(CdcLifecyclePoc.class);

  @DefaultCoder(SerializableCoder.class)
  public static class CdcRecord implements Serializable {
    public String id;
    public String payload;
    public String type; // INSERT, UPDATE, DELETE

    public CdcRecord() {}

    public CdcRecord(String id, String payload, String type) {
      this.id = id;
      this.payload = payload;
      this.type = type;
      this.tableName = "users"; // Default
    }

    public String tableName;
    public String parentId;
    public String rootId;

    public CdcRecord(String id, String payload, String type, String tableName, String parentId, String rootId) {
      this.id = id;
      this.payload = payload;
      this.type = type;
      this.tableName = tableName;
      this.parentId = parentId;
      this.rootId = rootId;
    }
  }

  @DefaultCoder(SerializableCoder.class)
  public static class LifecycleEvent implements Serializable {
      public String id;
      public String type; // UPDATE, DELETE
      public String payload;
      public String tableName;
      
      public LifecycleEvent() {}
      public LifecycleEvent(String id, String type, String payload, String tableName) {
          this.id = id;
          this.type = type;
          this.payload = payload;
          this.tableName = tableName;
      }
  }

  // Expansion DoFn: 1 Tick -> 13 Records (1 T1, 3 T2, 9 T3)
  static class ExpandToThreeTablesFn extends DoFn<Long, CdcRecord> {
      @ProcessElement
      public void processElement(@Element Long tick, OutputReceiver<CdcRecord> out) {
          // T1: 1 Record (Root is itself)
          String t1Id = java.util.UUID.randomUUID().toString();
          out.output(new CdcRecord(t1Id, "PAYLOAD-T1-" + tick, "INSERT", "T1", null, t1Id));

          // T2: 3 Records per T1 (Root is T1)
          for (int i = 0; i < 3; i++) {
              String t2Id = java.util.UUID.randomUUID().toString();
              out.output(new CdcRecord(t2Id, "PAYLOAD-T2-" + tick + "-" + i, "INSERT", "T2", t1Id, t1Id));

              // T3: 3 Records per T2 (Root is T1)
              for (int j = 0; j < 3; j++) {
                  String t3Id = java.util.UUID.randomUUID().toString();
                  out.output(new CdcRecord(t3Id, "PAYLOAD-T3-" + tick + "-" + i + "-" + j, "INSERT", "T3", t2Id, t1Id));
              }
          }
      }
  }

  public static void main(String[] args) {
    var options = PipelineOptionsFactory.fromArgs(args).withValidation().create();
    Pipeline p = Pipeline.create(options);

    // STEP 1: Generate "Ticks"
    // Rate: 2000 ticks/sec * 13 records/tick = 26,000 Total Ops/sec
    PCollection<Long> ticks = p.apply("GenerateTicks", GenerateSequence.from(0).withRate(2000, Duration.standardSeconds(1)));

    // STEP 2: Map to CdcRecord (INSERT) - Expansion to 3 Tables
    PCollection<CdcRecord> inserts = ticks
        .apply("ExpandToThreeTables", ParDo.of(new ExpandToThreeTablesFn()));

    // STEP 2: Stateful Lifecycle (Insert -> Wait -> Update -> Wait -> Delete)
    inserts
        .apply("ShardForLifecycle", WithKeys.of((CdcRecord r) -> String.valueOf(r.rootId.hashCode() % 5000))) // 5000 shards, grouped by ROOT ID
        .setCoder(KvCoder.of(StringUtf8Coder.of(), SerializableCoder.of(CdcRecord.class)))
        .apply("LifecycleSpannerWrites", ParDo.of(new LifecycleSpannerDoFn()));

    p.run();
  }

  // Stateful DoFn for Lifecycle: Insert -> Wait -> Update -> Wait -> Delete
  static class LifecycleSpannerDoFn extends DoFn<KV<String, CdcRecord>, CdcRecord> {
      private static final int BATCH_SIZE = 100;
      private static final int MAX_BUFFER_MILLIS = 1000;

      // Active Keys: MapState for efficient existence checks and removal
      @StateId("activeKeys")
      private final StateSpec<MapState<String, Boolean>> activeKeysSpec = StateSpecs.map();

      // Event Queue: Timestamp -> List of Events
      @StateId("eventQueue")
      private final StateSpec<ValueState<TreeMap<Long, List<LifecycleEvent>>>> eventQueueSpec = 
          StateSpecs.value(SerializableCoder.of((Class<TreeMap<Long, List<LifecycleEvent>>>)(Class)TreeMap.class));

      // Separate buffers for isolated batching
      @StateId("insertBuffer")
      private final StateSpec<BagState<CdcRecord>> insertBufferSpec = StateSpecs.bag(SerializableCoder.of(CdcRecord.class));
      @StateId("updateBuffer")
      private final StateSpec<BagState<CdcRecord>> updateBufferSpec = StateSpecs.bag(SerializableCoder.of(CdcRecord.class));
      @StateId("deleteBuffer")
      private final StateSpec<BagState<CdcRecord>> deleteBufferSpec = StateSpecs.bag(SerializableCoder.of(CdcRecord.class));

      @StateId("insertCount")
      private final StateSpec<ValueState<Integer>> insertCountSpec = StateSpecs.value();
      @StateId("updateCount")
      private final StateSpec<ValueState<Integer>> updateCountSpec = StateSpecs.value();
      @StateId("deleteCount")
      private final StateSpec<ValueState<Integer>> deleteCountSpec = StateSpecs.value();

      @TimerId("batchTimer")
      private final TimerSpec batchTimerSpec = TimerSpecs.timer(TimeDomain.PROCESSING_TIME);

      @TimerId("eventTimer")
      private final TimerSpec eventTimerSpec = TimerSpecs.timer(TimeDomain.PROCESSING_TIME);

      // Metrics
      private final Counter insertSuccess = Metrics.counter(LifecycleSpannerDoFn.class, "inserts_success");
      private final Counter insertFail = Metrics.counter(LifecycleSpannerDoFn.class, "inserts_fail");
      private final Counter updateSuccess = Metrics.counter(LifecycleSpannerDoFn.class, "updates_success");
      private final Counter updateFail = Metrics.counter(LifecycleSpannerDoFn.class, "updates_fail");
      private final Counter deleteSuccess = Metrics.counter(LifecycleSpannerDoFn.class, "deletes_success");
      private final Counter deleteFail = Metrics.counter(LifecycleSpannerDoFn.class, "deletes_fail");
      private final Counter duplicates = Metrics.counter(LifecycleSpannerDoFn.class, "duplicates");
      private final Counter batchFullRetries = Metrics.counter(LifecycleSpannerDoFn.class, "batch_full_retries");

      private transient com.google.cloud.spanner.Spanner spanner;
      private transient com.google.cloud.spanner.DatabaseClient dbClient;

      @Setup
      public void setup() {
          com.google.cloud.spanner.SpannerOptions options = com.google.cloud.spanner.SpannerOptions.newBuilder().build();
          spanner = options.getService();
          com.google.cloud.spanner.DatabaseId db = com.google.cloud.spanner.DatabaseId.of("span-cloud-testing", "shreya-test", "scale_test");
          dbClient = spanner.getDatabaseClient(db);
      }

      @Teardown
      public void teardown() {
          if (spanner != null) spanner.close();
      }

      @ProcessElement
      public void process(
              @Element KV<String, CdcRecord> kv,
              @StateId("activeKeys") MapState<String, Boolean> activeKeys,
              @StateId("eventQueue") ValueState<TreeMap<Long, List<LifecycleEvent>>> eventQueueState,
              @StateId("insertBuffer") BagState<CdcRecord> insertBuffer,
              @StateId("insertCount") ValueState<Integer> insertCount,
              @TimerId("batchTimer") Timer batchTimer,
              @TimerId("eventTimer") Timer eventTimer,
              OutputReceiver<CdcRecord> out) {
          
          CdcRecord record = kv.getValue();
          
          // Dedup check using MapState (Table-Aware)
          try {
              if (activeKeys.get(record.tableName + ":" + record.id).read() != null) {
                  duplicates.inc();
                  return; // Duplicate
              }
              // We DO NOT add to activeKeys here. We add ONLY after successful insert.
          } catch (Exception e) {
              LOG.error("Error checking active keys", e);
              return;
          }

          // Buffer Insert
          addToBuffer(record, insertBuffer, insertCount, batchTimer);
      }

      @OnTimer("eventTimer")
      public void onEventTimer(
              @StateId("activeKeys") MapState<String, Boolean> activeKeys,
              @StateId("eventQueue") ValueState<TreeMap<Long, List<LifecycleEvent>>> eventQueueState,
              @StateId("updateBuffer") BagState<CdcRecord> updateBuffer,
              @StateId("updateCount") ValueState<Integer> updateCount,
              @StateId("deleteBuffer") BagState<CdcRecord> deleteBuffer,
              @StateId("deleteCount") ValueState<Integer> deleteCount,
              @TimerId("batchTimer") Timer batchTimer,
              @TimerId("eventTimer") Timer eventTimer) {
          
          TreeMap<Long, List<LifecycleEvent>> queue = eventQueueState.read();
          if (queue == null) return;

          long now = System.currentTimeMillis();
          List<Long> processedTimestamps = new ArrayList<>();

          for (java.util.Map.Entry<Long, List<LifecycleEvent>> entry : queue.entrySet()) {
              if (entry.getKey() <= now) {
                  for (LifecycleEvent event : entry.getValue()) {
                       // Check if key is still active before scheduling update/delete
                       try {
                           if (activeKeys.get(event.tableName + ":" + event.id).read() == null) continue;
                       } catch (Exception e) { continue; }

                       if ("DELETE".equals(event.type)) {
                            addToBuffer(new CdcRecord(event.id, null, "DELETE", event.tableName, null, null), deleteBuffer, deleteCount, batchTimer);
                       } else if ("UPDATE".equals(event.type)) {
                            addToBuffer(new CdcRecord(event.id, event.payload, "UPDATE", event.tableName, null, null), updateBuffer, updateCount, batchTimer);
                       }
                  }
                  processedTimestamps.add(entry.getKey());
              } else {
                  // Queue is sorted, so we can stop
                  eventTimer.set(Instant.ofEpochMilli(entry.getKey()));
                  break;
              }
          }

          for (Long ts : processedTimestamps) {
              queue.remove(ts);
          }
          eventQueueState.write(queue);
      }

      @OnTimer("batchTimer")
      public void onBatchTimer(
              @StateId("insertBuffer") BagState<CdcRecord> insertBuffer,
              @StateId("insertCount") ValueState<Integer> insertCount,
              @StateId("updateBuffer") BagState<CdcRecord> updateBuffer,
              @StateId("updateCount") ValueState<Integer> updateCount,
              @StateId("deleteBuffer") BagState<CdcRecord> deleteBuffer,
              @StateId("deleteCount") ValueState<Integer> deleteCount,
              @StateId("activeKeys") MapState<String, Boolean> activeKeys,
              @StateId("eventQueue") ValueState<TreeMap<Long, List<LifecycleEvent>>> eventQueueState,
              @TimerId("eventTimer") Timer eventTimer,
              OutputReceiver<CdcRecord> out) {
          
          // Flush Inserts
          flushInserts(insertBuffer, insertCount, activeKeys, eventQueueState, eventTimer, out);
          
          // Flush Updates
          flushUpdates(updateBuffer, updateCount, out);
          
          // Flush Deletes
          flushDeletes(deleteBuffer, deleteCount, activeKeys, out);
      }

      private void addToBuffer(
              CdcRecord record, 
              BagState<CdcRecord> bufferState, 
              ValueState<Integer> countState, 
              Timer batchTimer) {
          bufferState.add(record);
          Integer count = countState.read();
          int newCount = (count == null ? 0 : count) + 1;
          countState.write(newCount);

          if (newCount == 1) {
              batchTimer.set(Instant.now().plus(Duration.millis(MAX_BUFFER_MILLIS)));
          }
          if (newCount >= BATCH_SIZE) {
              batchTimer.set(Instant.now());
          }
      }

      private void scheduleEvent(
              long timestamp, 
              LifecycleEvent event, 
              ValueState<TreeMap<Long, List<LifecycleEvent>>> eventQueueState, 
              Timer eventTimer) {
          TreeMap<Long, List<LifecycleEvent>> queue = eventQueueState.read();
          if (queue == null) queue = new TreeMap<>();
          
          queue.computeIfAbsent(timestamp, k -> new ArrayList<>()).add(event);
          eventQueueState.write(queue);

          // If this is the earliest event, set timer
          if (queue.firstKey().equals(timestamp)) {
              eventTimer.set(Instant.ofEpochMilli(timestamp));
          }
      }

      private void flushInserts(
              BagState<CdcRecord> buffer, 
              ValueState<Integer> countState,
              MapState<String, Boolean> activeKeys,
              ValueState<TreeMap<Long, List<LifecycleEvent>>> eventQueueState,
              Timer eventTimer,
              OutputReceiver<CdcRecord> out) {
          
          Iterable<CdcRecord> records = buffer.read();
          if (records == null || !records.iterator().hasNext()) return;

           List<CdcRecord> recordsList = new ArrayList<>();
           records.forEach(recordsList::add);
           // Sort for RI: Parent(1) -> Child(2) -> Grandchild(3)
           recordsList.sort(java.util.Comparator.comparingInt(this::getTablePriority));

           List<com.google.cloud.spanner.Mutation> mutations = new ArrayList<>();
           List<CdcRecord> batch = new ArrayList<>();
           java.util.Set<String> batchIds = new java.util.HashSet<>();

           for (CdcRecord r : recordsList) {
               if (batchIds.contains(r.tableName + ":" + r.id)) {
                   duplicates.inc();
                   continue;
               }
               batchIds.add(r.tableName + ":" + r.id);
               batch.add(r);
                com.google.cloud.spanner.Mutation.WriteBuilder builder = 
                    com.google.cloud.spanner.Mutation.newInsertBuilder(r.tableName)
                        .set("id").to(r.id)
                        .set("data").to(r.payload);
                if (r.parentId != null) {
                    builder.set("parent_id").to(r.parentId);
                }
                mutations.add(builder.build());
           }

          try {
              dbClient.writeAtLeastOnce(mutations);
              processSuccessfulInserts(batch, activeKeys, eventQueueState, eventTimer, out);
              insertSuccess.inc(batch.size());
          } catch (com.google.cloud.spanner.SpannerException e) {
              if (e.getErrorCode() == com.google.cloud.spanner.ErrorCode.ALREADY_EXISTS) {
                  LOG.warn("Batch failed with ALREADY_EXISTS. Retrying individually to adopt keys.");
                  int duplicateCountInBatch = 0;
                  for (CdcRecord r : batch) {
                      try {
                          com.google.cloud.spanner.Mutation.WriteBuilder builder = 
                              com.google.cloud.spanner.Mutation.newInsertBuilder(r.tableName)
                                  .set("id").to(r.id)
                                  .set("data").to(r.payload);
                          if (r.parentId != null) {
                              builder.set("parent_id").to(r.parentId);
                          }
                          dbClient.writeAtLeastOnce(java.util.Collections.singletonList(builder.build()));
                          processSuccessfulInserts(java.util.Collections.singletonList(r), activeKeys, eventQueueState, eventTimer, out);
                          insertSuccess.inc();
                      } catch (com.google.cloud.spanner.SpannerException e2) {
                          if (e2.getErrorCode() == com.google.cloud.spanner.ErrorCode.ALREADY_EXISTS) {
                              duplicates.inc();
                              duplicateCountInBatch++;
                              // ADOPT KEY: Schedule events anyway if user wants to track lifecycle
                              processSuccessfulInserts(java.util.Collections.singletonList(r), activeKeys, eventQueueState, eventTimer, out);
                          } else {
                              insertFail.inc();
                          }
                      } catch (Exception e3) {
                          insertFail.inc();
                      }
                  }
                  if (duplicateCountInBatch == batch.size()) {
                      batchFullRetries.inc();
                  }
              } else {
                  LOG.error("Spanner Insert Batch Failed", e);
                  insertFail.inc(batch.size());
              }
          } catch (Exception e) {
              LOG.error("Spanner Insert Batch Failed", e);
              insertFail.inc(batch.size());
              // FAIL: Do nothing. Keys not active, events not scheduled.
          }
          buffer.clear();
          countState.clear();
      }

      private void flushUpdates(BagState<CdcRecord> buffer, ValueState<Integer> countState, OutputReceiver<CdcRecord> out) {
          Iterable<CdcRecord> records = buffer.read();
          if (records == null || !records.iterator().hasNext()) return;
          
          List<com.google.cloud.spanner.Mutation> mutations = new ArrayList<>();
          List<CdcRecord> batch = new ArrayList<>();
          for (CdcRecord r : records) {
              batch.add(r);
              mutations.add(com.google.cloud.spanner.Mutation.newUpdateBuilder(r.tableName).set("id").to(r.id).set("data").to(r.payload).build());
          }
          try {
              dbClient.writeAtLeastOnce(mutations);
              for (CdcRecord r : batch) out.output(r);
              updateSuccess.inc(batch.size());
          } catch (Exception e) { 
              LOG.error("Spanner Update Batch Failed", e); 
              updateFail.inc(batch.size());
          }
          buffer.clear();
          countState.clear();
      }

      private void flushDeletes(
              BagState<CdcRecord> buffer, 
              ValueState<Integer> countState,
              MapState<String, Boolean> activeKeys,
              OutputReceiver<CdcRecord> out) {
          
          Iterable<CdcRecord> records = buffer.read();
          if (records == null || !records.iterator().hasNext()) return;

           List<CdcRecord> recordsList = new ArrayList<>();
           records.forEach(recordsList::add);
           // Sort for RI: Grandchild(3) -> Child(2) -> Parent(1)
           recordsList.sort(java.util.Comparator.comparingInt(this::getTablePriority).reversed());

           List<com.google.cloud.spanner.Mutation> mutations = new ArrayList<>();
           List<CdcRecord> batch = new ArrayList<>();
           for (CdcRecord r : recordsList) {
               batch.add(r);
               mutations.add(com.google.cloud.spanner.Mutation.delete(r.tableName, com.google.cloud.spanner.Key.of(r.id)));
           }
           try {
               dbClient.writeAtLeastOnce(mutations);
               // SUCCESS: Remove from active keys (Table-Aware)
               for (CdcRecord r : batch) {
                   activeKeys.remove(r.tableName + ":" + r.id);
                   out.output(r);
               }
              deleteSuccess.inc(batch.size());
          } catch (Exception e) { 
              LOG.error("Spanner Delete Batch Failed", e); 
              deleteFail.inc(batch.size());
          }
          buffer.clear();
          countState.clear();
      }

      private void processSuccessfulInserts(
              List<CdcRecord> batch, 
              MapState<String, Boolean> activeKeys,
              ValueState<TreeMap<Long, List<LifecycleEvent>>> eventQueueState,
              Timer eventTimer,
              OutputReceiver<CdcRecord> out) {
          
          long now = System.currentTimeMillis();
          for (CdcRecord r : batch) {
              activeKeys.put(r.tableName + ":" + r.id, true);
              // Update +5 min
              scheduleEvent(now + Duration.standardMinutes(5).getMillis(), new LifecycleEvent(r.id, "UPDATE", "UPDATED:" + r.payload, r.tableName), eventQueueState, eventTimer);
              
              // Delete +10 min (Only for 50% of SESSIONS - RootId based)
              if ((r.rootId.hashCode() & 1) == 0) {
                  scheduleEvent(now + Duration.standardMinutes(10).getMillis(), new LifecycleEvent(r.id, "DELETE", null, r.tableName), eventQueueState, eventTimer);
              }
              out.output(r);
          }
      }
      
      private int getTablePriority(CdcRecord r) {
          if ("T1".equals(r.tableName)) return 1;
          if ("T2".equals(r.tableName)) return 2;
          if ("T3".equals(r.tableName)) return 3;
          return 99; // Unknown
      }
  }
}
