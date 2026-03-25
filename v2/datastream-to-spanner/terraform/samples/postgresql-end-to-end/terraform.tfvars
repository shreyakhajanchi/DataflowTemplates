# Common Parameters
common_params = {
  project                         = "span-cloud-testing"      # Replace with your GCP project ID
  region                          = "us-central1"      # Replace with your desired GCP region
}

# Datastream Parameters
datastream_params = {
  source_connection_profile_id  = "source-postgresql" # Or provide a custom ID
  target_connection_profile_id  = "target-gcs"        # Or provide a custom ID
  target_gcs_bucket_name        = "live-migration"    # Or provide a custom bucket name
  pubsub_topic_name             = "live-migration"    # Or provide a custom topic name
  stream_id                     = "postgresql-stream" # Or provide a custom stream ID
  enable_backfill               = false                # This should always be enabled unless using sourcedb-to-spanner template for bulk migrations.
  max_concurrent_cdc_tasks      = 50                  # Adjust as needed
  max_concurrent_backfill_tasks = 50                  # Adjust as needed
  postgresql_host               = "34.9.243.181"
  # Use the Public IP if using IP allowlisting and Private IP if using
  # private connectivity.
  postgresql_username         = "datastream_user"
  postgresql_password         = "Pass@123"
  postgresql_publication      = "ds_pub"
  postgresql_replication_slot = "ds_replication_slot_new"
  postgresql_port             = 5432
  postgresql_database = {
    database = "customer_schema",
    schemas = [
      {
        schema_name = "public",
        tables      = ["chat_conversations"]
      }
    ]
  }
}

# Dataflow Parameters
dataflow_params = {
  skip_dataflow = false
  template_params = {
    create_shadow_tables                = true                              # true or false
    file_read_concurrency               = 10                                # Adjust as needed
    spanner_instance_id                 = "shreya-test"
    spanner_database_id                 = "customer-test"
    dlq_retry_minutes                   = 10                                             # Adjust as needed
    dlq_max_retry_count                 = 3                                              # Adjust as needed
    dead_letter_queue_directory         = "gs://khajanchi-gsql/dlq"                              # Optional dead letter queue directory (e.g., "gs://my-bucket/dlq")
    datastream_source_type              = "postgresql"
    round_json_decimals                 = false
    run_mode                            = "regular"
    spanner_priority                    = "HIGH"
    local_schema_overrides_file_path    = "schema-overrides.json"
  }

  runner_params = {
    additional_experiments       = []                            # Add any additional experiments or leave empty
    autoscaling_algorithm        = "AUTOSCALING_ALGORITHM_BASIC" # Acceptable values: AUTOSCALING_ALGORITHM_UNKNOWN / AUTOSCALING_ALGORITHM_BASIC / AUTOSCALING_ALGORITHM_NONE
    enable_streaming_engine      = true                          # Acceptable values: true / false
    labels                       = {}                            # Add any labels you want
    launcher_machine_type        = "n1-standard-1"               # Adjust as needed
    machine_type                 = "n2-standard-2"               # Adjust as needed
    max_workers                  = 10                            # Adjust based on your requirements
    job_name                     = "live-migration-job"          # Or your custom job name
    num_workers                  = 4                             # Adjust based on your requirements
    skip_wait_on_job_termination = false
    on_delete                    = "drain" # Acceptable values: drain / cancel
  }
}