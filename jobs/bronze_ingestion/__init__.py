# Bronze ingestion package - Kafka -> Avro decode -> MinIO Parquet (streaming).
#
# No eager imports here: the launcher (launcher.submit_all, run under plain
# python3) must not pull in pyspark, which only spark-submit's child process
# has on its path. Import KafkaReader etc. directly from their submodules.