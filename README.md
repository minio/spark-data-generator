# spark-data-generator
Generates dummy parquet, csv, json files for testing and validating MinIO S3 API compatibility.

## TODO
- Support configurable columns
- Support CSV, JSON

## Spark

Example shows how to generate parquet files for 1 billion rows, this example assumes that
you have configured `spark-defaults.conf` to talk to MinIO deployment.

```
~ sbt package
~ spark-submit --class "ParquetGenerator" --master spark://masternode:7077 \
    --packages org.apache.hadoop:hadoop-aws:3.1.2 --driver-memory 100G \
    --executor-memory 200G --total-executor-cores 256 \
    target/scala-2.12/simple-project_2.12-1.0.jar 1000000000 1000 s3a://benchmarks/1b/
```
