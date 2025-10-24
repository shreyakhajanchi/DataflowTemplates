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
package com.google.cloud.teleport.v2.templates;

import static com.google.cloud.teleport.v2.spanner.migrations.constants.Constants.MYSQL_SOURCE_TYPE;

import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.CqlSessionBuilder;
import com.datastax.oss.driver.api.core.config.DriverConfigLoader;
import com.google.cloud.Timestamp;
import com.google.cloud.teleport.metadata.Template;
import com.google.cloud.teleport.metadata.TemplateCategory;
import com.google.cloud.teleport.metadata.TemplateParameter;
import com.google.cloud.teleport.metadata.TemplateParameter.TemplateEnumOption;
import com.google.cloud.teleport.v2.cdc.dlq.DeadLetterQueueManager;
import com.google.cloud.teleport.v2.common.UncaughtExceptionLogger;
import com.google.cloud.teleport.v2.spanner.ddl.Ddl;
import com.google.cloud.teleport.v2.spanner.migrations.schema.ISchemaMapper;
import com.google.cloud.teleport.v2.spanner.migrations.schema.IdentityMapper;
import com.google.cloud.teleport.v2.spanner.migrations.schema.SchemaFileOverridesBasedMapper;
import com.google.cloud.teleport.v2.spanner.migrations.schema.SchemaStringOverridesBasedMapper;
import com.google.cloud.teleport.v2.spanner.migrations.schema.SessionBasedMapper;
import com.google.cloud.teleport.v2.spanner.migrations.shard.CassandraShard;
import com.google.cloud.teleport.v2.spanner.migrations.shard.Shard;
import com.google.cloud.teleport.v2.spanner.migrations.spanner.SpannerSchema;
import com.google.cloud.teleport.v2.spanner.migrations.utils.CassandraDriverConfigLoader;
import com.google.cloud.teleport.v2.spanner.sourceddl.CassandraInformationSchemaScanner;
import com.google.cloud.teleport.v2.spanner.sourceddl.MySqlInformationSchemaScanner;
import com.google.cloud.teleport.v2.spanner.sourceddl.SourceSchema;
import com.google.cloud.teleport.v2.spanner.sourceddl.SourceSchemaScanner;
import com.google.cloud.teleport.v2.templates.SpannerToSourceDb.Options;
import com.google.cloud.teleport.v2.templates.utils.ShadowTableCreator;
import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.List;
import org.apache.beam.runners.dataflow.options.DataflowPipelineDebugOptions;
import org.apache.beam.runners.dataflow.options.DataflowPipelineOptions;
import org.apache.beam.runners.dataflow.options.DataflowPipelineWorkerPoolOptions;
import org.apache.beam.runners.dataflow.options.DataflowPipelineWorkerPoolOptions.AutoscalingAlgorithmType;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.PipelineResult;
import org.apache.beam.sdk.coders.SerializableCoder;
import org.apache.beam.sdk.io.FileSystems;
import org.apache.beam.sdk.io.fs.ResolveOptions.StandardResolveOptions;
import org.apache.beam.sdk.io.gcp.spanner.SpannerAccessor;
import org.apache.beam.sdk.io.gcp.spanner.SpannerConfig;
import org.apache.beam.sdk.io.gcp.spanner.SpannerIO;
import org.apache.beam.sdk.io.gcp.spanner.changestreams.model.DataChangeRecord;
import org.apache.beam.sdk.options.Default;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.options.StreamingOptions;
import org.apache.beam.sdk.options.ValueProvider;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** This pipeline reads Spanner Change streams data and writes them to a source DB. */
@Template(
    name = "Spanner_to_SourceDb",
    category = TemplateCategory.STREAMING,
    displayName = "Spanner Change Streams to Source Database",
    description =
        "Streaming pipeline. Reads data from Spanner Change Streams and"
            + " writes them to a source.",
    optionsClass = Options.class,
    flexContainerName = "spanner-to-sourcedb",
    contactInformation = "https://cloud.google.com/support",
    hidden = false,
    streaming = true)
public class SpannerToSourceDb {

  private static final Logger LOG = LoggerFactory.getLogger(SpannerToSourceDb.class);

  /**
   * Options supported by the pipeline.
   *
   * <p>Inherits standard configuration options.
   */
  public interface Options extends PipelineOptions, StreamingOptions {

    @TemplateParameter.Text(
        order = 1,
        optional = false,
        description = "Name of the change stream to read from",
        helpText =
            "This is the name of the Spanner change stream that the pipeline will read from.")
    String getChangeStreamName();

    void setChangeStreamName(String value);

    @TemplateParameter.Text(
        order = 2,
        optional = false,
        description = "Cloud Spanner Instance Id.",
        helpText =
            "This is the name of the Cloud Spanner instance where the changestream is present.")
    String getInstanceId();

    void setInstanceId(String value);

    @TemplateParameter.Text(
        order = 3,
        optional = false,
        description = "Cloud Spanner Database Id.",
        helpText =
            "This is the name of the Cloud Spanner database that the changestream is monitoring")
    String getDatabaseId();

    void setDatabaseId(String value);

    @TemplateParameter.ProjectId(
        order = 4,
        optional = false,
        description = "Cloud Spanner Project Id.",
        helpText = "This is the name of the Cloud Spanner project.")
    String getSpannerProjectId();

    void setSpannerProjectId(String projectId);

    @TemplateParameter.Text(
        order = 5,
        optional = false,
        description = "Cloud Spanner Instance to store metadata when reading from changestreams",
        helpText =
            "This is the instance to store the metadata used by the connector to control the"
                + " consumption of the change stream API data.")
    String getMetadataInstance();

    void setMetadataInstance(String value);

    @TemplateParameter.Text(
        order = 6,
        optional = false,
        description = "Cloud Spanner Database to store metadata when reading from changestreams",
        helpText =
            "This is the database to store the metadata used by the connector to control the"
                + " consumption of the change stream API data.")
    String getMetadataDatabase();

    void setMetadataDatabase(String value);

    @TemplateParameter.Text(
        order = 7,
        optional = true,
        description = "Changes are read from the given timestamp",
        helpText = "Read changes from the given timestamp.")
    @Default.String("")
    String getStartTimestamp();

    void setStartTimestamp(String value);

    @TemplateParameter.Text(
        order = 8,
        optional = true,
        description = "Changes are read until the given timestamp",
        helpText =
            "Read changes until the given timestamp. If no timestamp provided, reads indefinitely.")
    @Default.String("")
    String getEndTimestamp();

    void setEndTimestamp(String value);

    @TemplateParameter.Text(
        order = 9,
        optional = true,
        description = "Cloud Spanner shadow table prefix.",
        helpText = "The prefix used to name shadow tables. Default: `shadow_`.")
    @Default.String("rev_shadow_")
    String getShadowTablePrefix();

    void setShadowTablePrefix(String value);

    @TemplateParameter.GcsReadFile(
        order = 10,
        optional = false,
        description = "Path to GCS file containing the the Source shard details",
        helpText = "Path to GCS file containing connection profile info for source shards.")
    String getSourceShardsFilePath();

    void setSourceShardsFilePath(String value);

    @TemplateParameter.GcsReadFile(
        order = 11,
        optional = true,
        description = "Session File Path in Cloud Storage",
        helpText =
            "Session file path in Cloud Storage that contains mapping information from"
                + " HarbourBridge")
    String getSessionFilePath();

    void setSessionFilePath(String value);

    @TemplateParameter.Enum(
        order = 12,
        optional = true,
        enumOptions = {@TemplateEnumOption("none"), @TemplateEnumOption("forward_migration")},
        description = "Filtration mode",
        helpText =
            "Mode of Filtration, decides how to drop certain records based on a criteria. Currently"
                + " supported modes are: none (filter nothing), forward_migration (filter records"
                + " written via the forward migration pipeline). Defaults to forward_migration.")
    @Default.String("forward_migration")
    String getFiltrationMode();

    void setFiltrationMode(String value);

    @TemplateParameter.GcsReadFile(
        order = 13,
        optional = true,
        description = "Custom jar location in Cloud Storage",
        helpText =
            "Custom jar location in Cloud Storage that contains the customization logic"
                + " for fetching shard id.")
    @Default.String("")
    String getShardingCustomJarPath();

    void setShardingCustomJarPath(String value);

    @TemplateParameter.Text(
        order = 14,
        optional = true,
        description = "Custom class name",
        helpText =
            "Fully qualified class name having the custom shard id implementation.  It is a"
                + " mandatory field in case shardingCustomJarPath is specified")
    @Default.String("")
    String getShardingCustomClassName();

    void setShardingCustomClassName(String value);

    @TemplateParameter.Text(
        order = 15,
        optional = true,
        description = "Custom sharding logic parameters",
        helpText =
            "String containing any custom parameters to be passed to the custom sharding class.")
    @Default.String("")
    String getShardingCustomParameters();

    void setShardingCustomParameters(String value);

    @TemplateParameter.Text(
        order = 16,
        optional = true,
        description = "SourceDB timezone offset",
        helpText =
            "This is the timezone offset from UTC for the source database. Example value: +10:00")
    @Default.String("+00:00")
    String getSourceDbTimezoneOffset();

    void setSourceDbTimezoneOffset(String value);

    @TemplateParameter.PubsubSubscription(
        order = 17,
        optional = true,
        description =
            "The Pub/Sub subscription being used in a Cloud Storage notification policy for DLQ"
                + " retry directory when running in regular mode.",
        helpText =
            "The Pub/Sub subscription being used in a Cloud Storage notification policy for DLQ"
                + " retry directory when running in regular mode. The name should be in the format"
                + " of projects/<project-id>/subscriptions/<subscription-name>. When set, the"
                + " deadLetterQueueDirectory and dlqRetryMinutes are ignored.")
    String getDlqGcsPubSubSubscription();

    void setDlqGcsPubSubSubscription(String value);

    @TemplateParameter.Text(
        order = 18,
        optional = true,
        description = "Directory name for holding skipped records",
        helpText =
            "Records skipped from reverse replication are written to this directory. Default"
                + " directory name is skip.")
    @Default.String("skip")
    String getSkipDirectoryName();

    void setSkipDirectoryName(String value);

    @TemplateParameter.Long(
        order = 19,
        optional = true,
        description = "Maximum connections per shard.",
        helpText = "This will come from shard file eventually.")
    @Default.Long(10000)
    Long getMaxShardConnections();

    void setMaxShardConnections(Long value);

    @TemplateParameter.Text(
        order = 20,
        optional = true,
        description = "Dead letter queue directory.",
        helpText =
            "The file path used when storing the error queue output. "
                + "The default file path is a directory under the Dataflow job's temp location.")
    @Default.String("")
    String getDeadLetterQueueDirectory();

    void setDeadLetterQueueDirectory(String value);

    @TemplateParameter.Integer(
        order = 21,
        optional = true,
        description = "Dead letter queue maximum retry count",
        helpText =
            "The max number of times temporary errors can be retried through DLQ. Defaults to 500.")
    @Default.Integer(500)
    Integer getDlqMaxRetryCount();

    void setDlqMaxRetryCount(Integer value);

    @TemplateParameter.Enum(
        order = 22,
        optional = true,
        description = "Run mode - currently supported are : regular or retryDLQ",
        enumOptions = {@TemplateEnumOption("regular"), @TemplateEnumOption("retryDLQ")},
        helpText =
            "This is the run mode type, whether regular or with retryDLQ.Default is regular."
                + " retryDLQ is used to retry the severe DLQ records only.")
    @Default.String("regular")
    String getRunMode();

    void setRunMode(String value);

    @TemplateParameter.Integer(
        order = 23,
        optional = true,
        description = "Dead letter queue retry minutes",
        helpText = "The number of minutes between dead letter queue retries. Defaults to 10.")
    @Default.Integer(10)
    Integer getDlqRetryMinutes();

    void setDlqRetryMinutes(Integer value);

    @TemplateParameter.Enum(
        order = 24,
        optional = true,
        description = "Source database type, ex: mysql",
        enumOptions = {@TemplateEnumOption("mysql"), @TemplateEnumOption("cassandra")},
        helpText = "The type of source database to reverse replicate to.")
    @Default.String("mysql")
    String getSourceType();

    void setSourceType(String value);

    @TemplateParameter.GcsReadFile(
        order = 25,
        optional = true,
        description = "Custom transformation jar location in Cloud Storage",
        helpText =
            "Custom jar location in Cloud Storage that contains the custom transformation logic for processing records"
                + " in reverse replication.")
    @Default.String("")
    String getTransformationJarPath();

    void setTransformationJarPath(String value);

    @TemplateParameter.Text(
        order = 26,
        optional = true,
        description = "Custom class name for transformation",
        helpText =
            "Fully qualified class name having the custom transformation logic.  It is a"
                + " mandatory field in case transformationJarPath is specified")
    @Default.String("")
    String getTransformationClassName();

    void setTransformationClassName(String value);

    @TemplateParameter.Text(
        order = 27,
        optional = true,
        description = "Custom parameters for transformation",
        helpText =
            "String containing any custom parameters to be passed to the custom transformation class.")
    @Default.String("")
    String getTransformationCustomParameters();

    void setTransformationCustomParameters(String value);

    @TemplateParameter.Text(
        order = 28,
        optional = true,
        description = "Table name overrides from spanner to source",
        regexes =
            "^\\[([[:space:]]*\\{[[:graph:]]+[[:space:]]*,[[:space:]]*[[:graph:]]+[[:space:]]*\\}[[:space:]]*(,[[:space:]]*)*)*\\]$",
        example = "[{Singers, Vocalists}, {Albums, Records}]",
        helpText =
            "These are the table name overrides from spanner to source. They are written in the"
                + "following format: [{SpannerTableName1, SourceTableName1}, {SpannerTableName2, SourceTableName2}]"
                + "This example shows mapping Singers table to Vocalists and Albums table to Records.")
    @Default.String("")
    String getTableOverrides();

    void setTableOverrides(String value);

    @TemplateParameter.Text(
        order = 29,
        optional = true,
        description = "Column name overrides from spanner to source",
        regexes =
            "^\\[([[:space:]]*\\{[[:space:]]*[[:graph:]]+\\.[[:graph:]]+[[:space:]]*,[[:space:]]*[[:graph:]]+\\.[[:graph:]]+[[:space:]]*\\}[[:space:]]*(,[[:space:]]*)*)*\\]$",
        example =
            "[{Singers.SingerName, Singers.TalentName}, {Albums.AlbumName, Albums.RecordName}]",
        helpText =
            "These are the column name overrides from spanner to source. They are written in the"
                + "following format: [{SpannerTableName1.SpannerColumnName1, SpannerTableName1.SourceColumnName1}, {SpannerTableName2.SpannerColumnName1, SpannerTableName2.SourceColumnName1}]"
                + "Note that the SpannerTableName should remain the same in both the spanner and source pair. To override table names, use tableOverrides."
                + "The example shows mapping SingerName to TalentName and AlbumName to RecordName in Singers and Albums table respectively.")
    @Default.String("")
    String getColumnOverrides();

    void setColumnOverrides(String value);

    @TemplateParameter.GcsReadFile(
        order = 30,
        optional = true,
        description = "File based overrides from spanner to source",
        helpText =
            "A file which specifies the table and the column name overrides from spanner to source.")
    @Default.String("")
    String getSchemaOverridesFilePath();

    void setSchemaOverridesFilePath(String value);

    @TemplateParameter.Text(
        order = 31,
        optional = true,
        description = "Directory name for holding filtered records",
        helpText =
            "Records skipped from reverse replication are written to this directory. Default"
                + " directory name is skip.")
    @Default.String("filteredEvents")
    String getFilterEventsDirectoryName();

    void setFilterEventsDirectoryName(String value);

    @TemplateParameter.Boolean(
        order = 32,
        optional = true,
        description = "Boolean setting if reverse migration is sharded",
        helpText =
            "Sets the template to a sharded migration. If source shard template contains more"
                + " than one shard, the value will be set to true. This value defaults to false.")
    @Default.Boolean(false)
    Boolean getIsShardedMigration();

    void setIsShardedMigration(Boolean value);

    @TemplateParameter.Text(
        order = 33,
        optional = true,
        description = "Failure injection parameter",
        helpText = "Failure injection parameter. Only used for testing.")
    @Default.String("")
    String getFailureInjectionParameter();

    void setFailureInjectionParameter(String value);
  }

  /**
   * Main entry point for executing the pipeline.
   *
   * @param args The command-line arguments to the pipeline.
   */
  public static void main(String[] args) {
    UncaughtExceptionLogger.register();

    LOG.info("Starting Spanner change streams to sink");

    Options options = PipelineOptionsFactory.fromArgs(args).withValidation().as(Options.class);

    options.setStreaming(true);

    run(options);
  }

  /**
   * Runs the pipeline with the supplied options.
   *
   * @param options The execution parameters to the pipeline.
   * @return The result of the pipeline execution.
   */
  public static PipelineResult run(Options options) {

    Pipeline pipeline = Pipeline.create(options);
    pipeline
        .getOptions()
        .as(DataflowPipelineWorkerPoolOptions.class)
        .setAutoscalingAlgorithm(AutoscalingAlgorithmType.THROUGHPUT_BASED);

    // calculate the max connections per worker
    int maxNumWorkers =
        pipeline.getOptions().as(DataflowPipelineWorkerPoolOptions.class).getMaxNumWorkers() > 0
            ? pipeline.getOptions().as(DataflowPipelineWorkerPoolOptions.class).getMaxNumWorkers()
            : 1;
    int connectionPoolSizePerWorker = (int) (options.getMaxShardConnections() / maxNumWorkers);
    LOG.info("Connection pool size per worker" + connectionPoolSizePerWorker);
    if (connectionPoolSizePerWorker < 1) {
      // This can happen when the number of workers is more than max.
      // This can cause overload on the source database. Error out and let the user know.
      LOG.error(
          "Max workers {} is more than max shard connections {}, this can lead to more database"
              + " connections than desired",
          maxNumWorkers,
          options.getMaxShardConnections());
      throw new IllegalArgumentException(
          "Max Dataflow workers "
              + maxNumWorkers
              + " is more than max per shard connections: "
              + options.getMaxShardConnections()
              + " this can lead to more"
              + " database connections than desired. Either reduce the max allowed workers or"
              + " incease the max shard connections");
    }

    String stagingHost = "https://preprod-spanner.sandbox.googleapis.com";
    // Prepare Spanner config
    SpannerConfig spannerConfig =
        SpannerConfig.create()
            .withProjectId(ValueProvider.StaticValueProvider.of(options.getSpannerProjectId()))
            .withInstanceId(ValueProvider.StaticValueProvider.of(options.getInstanceId()))
            .withDatabaseId(ValueProvider.StaticValueProvider.of(options.getDatabaseId()));

    // Create shadow tables
    // Note that there is a limit on the number of tables that can be created per DB: 5000.
    // If we create shadow tables per shard, there will be an explosion of tables.
    // Anyway the shadow table has Spanner PK so no need to again separate by the shard
    // Lookup by the Spanner PK should be sufficient.

    // Prepare Spanner config
    SpannerConfig spannerMetadataConfig =
        SpannerConfig.create()
            .withProjectId(ValueProvider.StaticValueProvider.of(options.getSpannerProjectId()))
            .withInstanceId(ValueProvider.StaticValueProvider.of(options.getMetadataInstance()))
            .withDatabaseId(ValueProvider.StaticValueProvider.of(options.getMetadataDatabase()));

    ShadowTableCreator shadowTableCreator =
        new ShadowTableCreator(
            spannerConfig,
            spannerMetadataConfig,
            SpannerAccessor.getOrCreate(spannerMetadataConfig)
                .getDatabaseAdminClient()
                .getDatabase(
                    spannerMetadataConfig.getInstanceId().get(),
                    spannerMetadataConfig.getDatabaseId().get())
                .getDialect(),
            options.getShadowTablePrefix());

    DataflowPipelineDebugOptions debugOptions = options.as(DataflowPipelineDebugOptions.class);

    shadowTableCreator.createShadowTablesInSpanner();
    Ddl ddl = SpannerSchema.getInformationSchemaAsDdl(spannerConfig);
    List<Shard> shards;
    String shardingMode;
    pipeline
        .getCoderRegistry()
        .registerCoderForClass(
            DataChangeRecord.class, SerializableCoder.of(DataChangeRecord.class));

    pipeline.apply(
        getReadChangeStreamDoFn(
            options, spannerConfig)); // This emits PCollection<DataChangeRecord> which is Spanner

    return pipeline.run();
  }

  public static SpannerIO.ReadChangeStream getReadChangeStreamDoFn(
      Options options, SpannerConfig spannerConfig) {

    Timestamp startTime = Timestamp.now();
    if (!options.getStartTimestamp().equals("")) {
      startTime = Timestamp.parseTimestamp(options.getStartTimestamp());
    }
    SpannerIO.ReadChangeStream readChangeStreamDoFn =
        SpannerIO.readChangeStream()
            .withSpannerConfig(spannerConfig)
            .withChangeStreamName(options.getChangeStreamName())
            .withMetadataInstance(options.getMetadataInstance())
            .withMetadataDatabase(options.getMetadataDatabase())
            .withInclusiveStartAt(startTime);
    if (!options.getEndTimestamp().equals("")) {
      return readChangeStreamDoFn.withInclusiveEndAt(
          Timestamp.parseTimestamp(options.getEndTimestamp()));
    }
    return readChangeStreamDoFn;
  }

  private static DeadLetterQueueManager buildDlqManager(Options options) {
    String tempLocation =
        options.as(DataflowPipelineOptions.class).getTempLocation().endsWith("/")
            ? options.as(DataflowPipelineOptions.class).getTempLocation()
            : options.as(DataflowPipelineOptions.class).getTempLocation() + "/";
    String dlqDirectory =
        options.getDeadLetterQueueDirectory().isEmpty()
            ? tempLocation + "dlq/"
            : options.getDeadLetterQueueDirectory();
    LOG.info("Dead-letter queue directory: {}", dlqDirectory);
    options.setDeadLetterQueueDirectory(dlqDirectory);
    if ("regular".equals(options.getRunMode())) {
      return DeadLetterQueueManager.create(dlqDirectory, options.getDlqMaxRetryCount());
    } else {
      String retryDlqUri =
          FileSystems.matchNewResource(dlqDirectory, true)
              .resolve("severe", StandardResolveOptions.RESOLVE_DIRECTORY)
              .toString();
      LOG.info("Dead-letter retry directory: {}", retryDlqUri);
      return DeadLetterQueueManager.create(dlqDirectory, retryDlqUri, 0);
    }
  }

  private static Connection createJdbcConnection(Shard shard) {
    try {
      String sourceConnectionUrl =
          "jdbc:mysql://" + shard.getHost() + ":" + shard.getPort() + "/" + shard.getDbName();
      HikariConfig config = new HikariConfig();
      config.setJdbcUrl(sourceConnectionUrl);
      config.setUsername(shard.getUserName());
      config.setPassword(shard.getPassword());
      config.setDriverClassName("com.mysql.cj.jdbc.Driver");
      HikariDataSource ds = new HikariDataSource(config);
      return ds.getConnection();
    } catch (java.sql.SQLException e) {
      LOG.error("Sql error while discovering mysql schema: " + e);
      throw new RuntimeException(e);
    }
  }

  /**
   * Creates a {@link CqlSession} for the given {@link CassandraShard}.
   *
   * @param cassandraShard The shard containing connection details.
   * @return A {@link CqlSession} instance.
   */
  private static CqlSession createCqlSession(CassandraShard cassandraShard) {
    CqlSessionBuilder builder = CqlSession.builder();
    DriverConfigLoader configLoader =
        CassandraDriverConfigLoader.fromOptionsMap(cassandraShard.getOptionsMap());
    builder.withConfigLoader(configLoader);
    return builder.build();
  }

  private static SourceSchema fetchSourceSchema(Options options, List<Shard> shards) {
    SourceSchemaScanner scanner = null;
    SourceSchema sourceSchema = null;
    try {
      if (options.getSourceType().equals(MYSQL_SOURCE_TYPE)) {
        Connection connection = createJdbcConnection(shards.get(0));
        scanner = new MySqlInformationSchemaScanner(connection, shards.get(0).getDbName());
        sourceSchema = scanner.scan();
        connection.close();
      } else {
        try (CqlSession session = createCqlSession((CassandraShard) shards.get(0))) {
          scanner =
              new CassandraInformationSchemaScanner(
                  session, ((CassandraShard) shards.get(0)).getKeySpaceName());
          sourceSchema = scanner.scan();
        }
      }
    } catch (SQLException e) {
      throw new RuntimeException("Unable to discover jdbc schema", e);
    }
    return sourceSchema;
  }

  private static ISchemaMapper getSchemaMapper(Options options, Ddl ddl) {
    // Check if config types are specified
    boolean hasSessionFile =
        options.getSessionFilePath() != null && !options.getSessionFilePath().equals("");
    boolean hasSchemaOverridesFile =
        options.getSchemaOverridesFilePath() != null
            && !options.getSchemaOverridesFilePath().equals("");
    boolean hasStringOverrides =
        (options.getTableOverrides() != null && !options.getTableOverrides().equals(""))
            || (options.getColumnOverrides() != null && !options.getColumnOverrides().equals(""));

    int overrideTypesCount = 0;
    if (hasSessionFile) {
      overrideTypesCount++;
    }
    if (hasSchemaOverridesFile) {
      overrideTypesCount++;
    }
    if (hasStringOverrides) {
      overrideTypesCount++;
    }

    if (overrideTypesCount > 1) {
      throw new IllegalArgumentException(
          "Only one type of schema override can be specified. Please use only one of: sessionFilePath, "
              + "schemaOverridesFilePath, or tableOverrides/columnOverrides.");
    }

    ISchemaMapper schemaMapper = new IdentityMapper(ddl);
    if (hasSessionFile) {
      schemaMapper = new SessionBasedMapper(options.getSessionFilePath(), ddl);
    } else if (hasSchemaOverridesFile) {
      schemaMapper = new SchemaFileOverridesBasedMapper(options.getSchemaOverridesFilePath(), ddl);
    } else if (hasStringOverrides) {
      schemaMapper =
          new SchemaStringOverridesBasedMapper(
              options.getTableOverrides(), options.getColumnOverrides(), ddl);
    }
    return schemaMapper;
  }
}
