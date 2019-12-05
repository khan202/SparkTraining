package SparkSQL

import com.typesafe.config.ConfigFactory
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._


object KaiOSExcercise {

  def main(args: Array[String]): Unit = {

    val conf = ConfigFactory.load("application.conf")
    val spark = SparkSession.builder().appName(conf.getString("appName"))
      .config("spark.executor.memory", conf.getString("executorMemory"))
      .config("spark.master", conf.getString("master"))
      .config("spark.driver.memory", conf.getString("driverMemory"))
      .getOrCreate()

    val data = spark.read.format("com.databricks.spark.csv")
      .option("delimiter", "\t")
      .option("header", false)
      .option("inferSchema", true).load(conf.getString("DATA_DIR")+conf.getString("transactions"))//"G:\\Spark\\lastfm-dataset-1K\\userid-timestamp-artid-artname-traid-traname.tsv")
      .toDF("userid", "timestamp", "artistid", "artistname", "trackid", "trackname")


    import spark.implicits._
    val distinctUsers = data.select("userid", "trackid").distinct()
      .groupBy("userid").agg(count("userid").alias("distincttracks")).orderBy($"distincttracks".desc)

    // distinctUsers.write.format("csv").option("delimeter","\t").save(conf.getString("DATA_DIR")+"solution-1-result")
    distinctUsers.coalesce(1)
      .write.format("com.databricks.spark.csv")
      .option("header", "true")
      .save(conf.getString("DATA_DIR") + "solution-1-result")

  }

}
