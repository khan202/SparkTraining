package SparkSQL
import com.typesafe.config.ConfigFactory
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._


object KaiOSExcercise {

  def main(args: Array[String]): Unit = {

    val conf = ConfigFactory.load("application.conf")
    val spark = SparkSession.builder().appName(conf.getString("appName"))
      .config("spark.executor.memory", conf.getString("executorMemory"))
      .config("master", conf.getString("master"))
      .config("spark.driver.memory", conf.getString("driverMemory"))
      .getOrCreate()

    val data = spark.read.format("com.databricks.spark.csv")
      .option("delimiter", "\t")
      .option("header", false)
      .option("inferSchema", true).load("G:\\Spark\\lastfm-dataset-1K\\userid-timestamp-artid-artname-traid-traname.tsv")
      .toDF("userid", "timestamp", "musicbrainz-artist-id", "artist-name", "musicbrainz-track-id", "track-name")

    //data.show()
    import spark.implicits._
    val distinctUsers = data.select("userid", "musicbrainz-track-id").distinct()



    //distinctUsers.show()

   distinctUsers.select($"userid", count($"userid").alias("distinctsongs").desc).show()

//distinctUserIds.select($"userid", count($"userid").alias("distinctsongs")).groupBy("userid")


  }

}
