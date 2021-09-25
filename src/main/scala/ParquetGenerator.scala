import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.SaveMode
import org.apache.spark.sql.Row
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions.{to_utc_timestamp, from_unixtime, monotonically_increasing_id, to_date}

object ParquetGenerator {
  def main(args: Array[String]) {

    val rng = args(0).toInt
    val parts = args(1).toInt
    val path = args(2)
    val spark = SparkSession.builder.appName("Data Generator").getOrCreate()

    def randomStringGen(length: Int) = scala.util.Random.alphanumeric.take(length).mkString

    // This is an example, please configure this properly for your local
    // setup at spark-defaults.conf
    // spark.sparkContext.hadoopConfiguration.set("fs.s3a.access.key", "minio")
    // spark.sparkContext.hadoopConfiguration.set("fs.s3a.secret.key", "minio123")
    // spark.sparkContext.hadoopConfiguration.set("fs.s3a.endpoint", "http://127.0.0.1:9000")
    // spark.sparkContext.hadoopConfiguration.set("mapreduce.outputcommitter.factory.scheme.s3a",
    //                                            "org.apache.hadoop.fs.s3a.commit.S3ACommitterFactory")
    // spark.sparkContext.hadoopConfiguration.set("spark.hadoop.mapreduce.outputcommitter.factory.scheme.s3a",
    //                                            "org.apache.hadoop.fs.s3a.commit.S3ACommitterFactory")
    // spark.sparkContext.hadoopConfiguration.set("spark.hadoop.fs.s3a.committer.name", "magic")
    // spark.sparkContext.hadoopConfiguration.set("spark.hadoop.fs.s3a.committer.magic.enabled", "true")
    // spark.sparkContext.hadoopConfiguration.set("mapreduce.fileoutputcommitter.algorithm.version", "2")
    // spark.sparkContext.hadoopConfiguration.set("fs.s3a.committer.magic.enabled", "true")
    // spark.sparkContext.hadoopConfiguration.set("fs.s3a.committer.name", "magic")
    // spark.sparkContext.hadoopConfiguration.set("fs.s3a.buffer.dir", "file:///tmp/staging")
    // spark.sparkContext.hadoopConfiguration.set("fs.s3a.fast.upload.buffer", "disk")
    // spark.sparkContext.hadoopConfiguration.set("fs.s3a.fast.upload.active.blocks", "8")
    // spark.sparkContext.hadoopConfiguration.set("fs.s3a.committer.threads", "8")

    val rdd = spark.sparkContext.parallelize(1 to rng, parts)
      .map(x => Row(randomStringGen(4), randomStringGen(4),
        randomStringGen(6), randomStringGen(6), randomStringGen(6),
        randomStringGen(6), randomStringGen(6), randomStringGen(6),
        randomStringGen(6), randomStringGen(6)
      )
    )

    val schema = StructType(Seq(
      StructField("_1", StringType),
      StructField("_2", StringType),
      StructField("_3", StringType),
      StructField("_4", StringType),
      StructField("_5", StringType),
      StructField("_6", StringType),
      StructField("_7", StringType),
      StructField("_8", StringType),
      StructField("_9", StringType),
      StructField("_10", StringType)))

    val df = spark.createDataFrame(rdd, schema)
      .withColumn("id", monotonically_increasing_id())
      .withColumnRenamed("_1", "coln_2028_1")
      .withColumnRenamed("_2", "coln_2028_2")
      .withColumnRenamed("_3", "coln_2028_3")
      .withColumnRenamed("_4", "coln_2028_4")
      .withColumnRenamed("_5", "coln_2028_5")
      .withColumnRenamed("_6", "coln_2028_6")
      .withColumnRenamed("_7", "coln_2028_7")
      .withColumnRenamed("_8", "coln_2028_8")
      .withColumnRenamed("_9", "coln_2028_9")
      .withColumnRenamed("_10", "coln_2028_10")
    
    df.write.mode(SaveMode.Overwrite).parquet(path)
    spark.stop()
  }
}
