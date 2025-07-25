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
package com.google.cloud.teleport.v2.source.reader.io.cassandra.iowrapper;

import static com.google.cloud.teleport.v2.source.reader.io.cassandra.testutils.BasicTestSchema.BASIC_TEST_TABLE;
import static com.google.cloud.teleport.v2.source.reader.io.cassandra.testutils.BasicTestSchema.PRIMITIVE_TYPES_TABLE;
import static com.google.cloud.teleport.v2.source.reader.io.cassandra.testutils.BasicTestSchema.TEST_KEYSPACE;
import static com.google.common.truth.Truth.assertThat;
import static org.mockito.Mockito.mockStatic;

import com.datastax.oss.driver.api.core.config.OptionsMap;
import com.datastax.oss.driver.api.core.config.TypedDriverOption;
import com.google.cloud.teleport.v2.source.reader.auth.dbauth.GuardedStringValueProvider;
import com.google.cloud.teleport.v2.source.reader.io.cassandra.iowrapper.CassandraDataSource.CassandraDialect;
import com.google.cloud.teleport.v2.source.reader.io.cassandra.schema.CassandraSchemaReference;
import com.google.cloud.teleport.v2.source.reader.io.datasource.DataSource;
import com.google.cloud.teleport.v2.source.reader.io.row.SourceRow;
import com.google.cloud.teleport.v2.source.reader.io.schema.SchemaDiscovery;
import com.google.cloud.teleport.v2.source.reader.io.schema.SourceSchema;
import com.google.cloud.teleport.v2.source.reader.io.schema.SourceSchemaReference;
import com.google.cloud.teleport.v2.source.reader.io.schema.SourceTableReference;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import java.net.InetSocketAddress;
import org.apache.beam.sdk.io.cassandra.CassandraIO;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.values.PBegin;
import org.apache.beam.sdk.values.PCollection;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.MockedStatic;
import org.mockito.Mockito;
import org.mockito.junit.MockitoJUnitRunner;

/** Test class for {@link CassandraIoWrapper}. */
@RunWith(MockitoJUnitRunner.class)
public class CassandraIoWrapperTest {

  @Test
  public void testCassandraIoWrapperBasic() {
    String testClusterName = "testCluster";
    InetSocketAddress testHost = new InetSocketAddress("127.0.0.1", 9042);
    String testLocalDC = "datacenter1";
    DataSource ossDataSource =
        DataSource.ofCassandra(
            CassandraDataSource.ofOss(
                CassandraDataSourceOss.builder()
                    .setOptionsMap(OptionsMap.driverDefaults())
                    .setClusterName(testClusterName)
                    .setContactPoints(ImmutableList.of(testHost))
                    .setLocalDataCenter(testLocalDC)
                    .overrideOptionInOptionsMap(TypedDriverOption.SESSION_KEYSPACE, TEST_KEYSPACE)
                    .build()));
    DataSource astraDataSource =
        DataSource.ofCassandra(
            CassandraDataSource.ofAstra(
                AstraDbDataSource.builder()
                    .setAstraToken("AstraCS:testToken")
                    .setDatabaseId("testID")
                    .setKeySpace(TEST_KEYSPACE)
                    .setAstraDbRegion("testRegion")
                    .build()));

    SourceSchemaReference sourceSchemaReference =
        SourceSchemaReference.ofCassandra(
            CassandraSchemaReference.builder()
                .setKeyspaceName(ossDataSource.cassandra().loggedKeySpace())
                .build());

    String testGcsPath = "gs://smt-test-bucket/cassandraConfig.conf";
    SchemaDiscovery schemaDiscovery = CassandraIOWrapperHelper.buildSchemaDiscovery();
    ImmutableList<String> tablesToRead = ImmutableList.of(BASIC_TEST_TABLE, PRIMITIVE_TYPES_TABLE);
    SourceSchema mockSourceSchema = Mockito.mock(SourceSchema.class);
    SourceTableReference mockSourceTableReference = Mockito.mock(SourceTableReference.class);
    CassandraIO.Read<SourceRow> mockTableReader = Mockito.mock(CassandraIO.Read.class);
    ImmutableMap<SourceTableReference, PTransform<PBegin, PCollection<SourceRow>>>
        mockTableReaders = ImmutableMap.of(mockSourceTableReference, mockTableReader);

    try (MockedStatic mockCassandraIoWrapperHelper = mockStatic(CassandraIOWrapperHelper.class)) {
      mockCassandraIoWrapperHelper
          .when(
              () ->
                  CassandraIOWrapperHelper.buildDataSource(
                      testGcsPath,
                      null,
                      CassandraDialect.OSS,
                      GuardedStringValueProvider.create(""),
                      "",
                      "",
                      ""))
          .thenReturn(ossDataSource);
      mockCassandraIoWrapperHelper
          .when(
              () ->
                  CassandraIOWrapperHelper.buildDataSource(
                      "",
                      null,
                      CassandraDialect.ASTRA,
                      astraDataSource.cassandra().astra().astraToken(),
                      astraDataSource.cassandra().astra().databaseId(),
                      astraDataSource.cassandra().astra().keySpace(),
                      astraDataSource.cassandra().astra().astraDbRegion()))
          .thenReturn(astraDataSource);
      mockCassandraIoWrapperHelper
          .when(() -> CassandraIOWrapperHelper.buildSchemaDiscovery())
          .thenReturn(schemaDiscovery);
      mockCassandraIoWrapperHelper
          .when(
              () ->
                  CassandraIOWrapperHelper.getTablesToRead(
                      tablesToRead, ossDataSource, schemaDiscovery, sourceSchemaReference))
          .thenReturn(tablesToRead);
      mockCassandraIoWrapperHelper
          .when(
              () ->
                  CassandraIOWrapperHelper.getTablesToRead(
                      tablesToRead, astraDataSource, schemaDiscovery, sourceSchemaReference))
          .thenReturn(tablesToRead);
      mockCassandraIoWrapperHelper
          .when(
              () ->
                  CassandraIOWrapperHelper.getSourceSchema(
                      schemaDiscovery, ossDataSource, sourceSchemaReference, tablesToRead))
          .thenReturn(mockSourceSchema);
      mockCassandraIoWrapperHelper
          .when(
              () ->
                  CassandraIOWrapperHelper.getSourceSchema(
                      schemaDiscovery, astraDataSource, sourceSchemaReference, tablesToRead))
          .thenReturn(mockSourceSchema);
      mockCassandraIoWrapperHelper
          .when(() -> CassandraIOWrapperHelper.getTableReaders(ossDataSource, mockSourceSchema))
          .thenReturn(mockTableReaders);
      mockCassandraIoWrapperHelper
          .when(() -> CassandraIOWrapperHelper.getTableReaders(astraDataSource, mockSourceSchema))
          .thenReturn(mockTableReaders);

      CassandraIoWrapper cassandraIoWrapper =
          new CassandraIoWrapper(
              testGcsPath,
              tablesToRead,
              null,
              CassandraDialect.OSS,
              GuardedStringValueProvider.create(""),
              "",
              "",
              "");
      assertThat(cassandraIoWrapper.discoverTableSchema()).isEqualTo(mockSourceSchema);
      assertThat(cassandraIoWrapper.getTableReaders()).isEqualTo(mockTableReaders);

      CassandraIoWrapper cassandraIoWrapperAstra =
          new CassandraIoWrapper(
              "",
              tablesToRead,
              null,
              CassandraDialect.ASTRA,
              astraDataSource.cassandra().astra().astraToken(),
              astraDataSource.cassandra().astra().databaseId(),
              astraDataSource.cassandra().astra().keySpace(),
              astraDataSource.cassandra().astra().astraDbRegion());
      assertThat(cassandraIoWrapperAstra.discoverTableSchema()).isEqualTo(mockSourceSchema);
      assertThat(cassandraIoWrapperAstra.getTableReaders()).isEqualTo(mockTableReaders);
    }
  }
}
