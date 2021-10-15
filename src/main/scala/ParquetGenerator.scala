import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.SaveMode
import org.apache.spark.sql.Row
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions.{to_utc_timestamp, from_unixtime, monotonically_increasing_id, to_date}

object ParquetGenerator {
  def main(args: Array[String]) {

    val rng = args(0).toInt
    val parts = args(1).toInt
    var columns = args(2).toInt
    val path = args(3)
    val spark = SparkSession.builder.appName("Data Generator").getOrCreate()

    def randomStringGen(length: Int) = scala.util.Random.alphanumeric.take(length).mkString

    // spark.sparkContext.hadoopConfiguration.set("fs.s3a.access.key", "minio")
    // spark.sparkContext.hadoopConfiguration.set("fs.s3a.secret.key", "minio123")
    // spark.sparkContext.hadoopConfiguration.set("fs.s3a.endpoint", "http://127.0.0.1:9000")
    // spark.sparkContext.hadoopConfiguration.set("mapreduce.outputcommitter.factory.scheme.s3a",
    //   "org.apache.hadoop.fs.s3a.commit.S3ACommitterFactory")
    // spark.sparkContext.hadoopConfiguration.set("spark.hadoop.mapreduce.outputcommitter.factory.scheme.s3a",
    //   "org.apache.hadoop.fs.s3a.commit.S3ACommitterFactory")
    // spark.sparkContext.hadoopConfiguration.set("spark.hadoop.fs.s3a.committer.name", "magic")
    // spark.sparkContext.hadoopConfiguration.set("spark.hadoop.fs.s3a.committer.magic.enabled", "true")
    // spark.sparkContext.hadoopConfiguration.set("mapreduce.fileoutputcommitter.algorithm.version", "2")
    // spark.sparkContext.hadoopConfiguration.set("fs.s3a.committer.magic.enabled", "true")
    // spark.sparkContext.hadoopConfiguration.set("fs.s3a.committer.name", "magic")
    // spark.sparkContext.hadoopConfiguration.set("fs.s3a.buffer.dir", "file:///tmp/staging")
    // spark.sparkContext.hadoopConfiguration.set("fs.s3a.fast.upload.buffer", "disk")
    // spark.sparkContext.hadoopConfiguration.set("fs.s3a.fast.upload.active.blocks", "64")
    // spark.sparkContext.hadoopConfiguration.set("fs.s3a.committer.threads", "64")

    val rdd = spark.sparkContext.parallelize(1 to rng, parts).map(x => {
      var values = Seq[String]()
      for (i <- 1 to columns)
	values = values :+ randomStringGen(6)
      Row.fromSeq(values)
    })

    var fields = Seq[StructField]()
    for (i <- 1 to columns)
      fields = fields :+ StructField("col_"+i.toString, StringType, true)

    val df = spark.createDataFrame(rdd, StructType(fields))
      .withColumn("id", monotonically_increasing_id())

    df.write.mode(SaveMode.Overwrite).parquet(path)
    spark.stop()
  }
}
