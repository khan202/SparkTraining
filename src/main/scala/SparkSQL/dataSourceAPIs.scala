package SparkSQL

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

/**
  * Created by ImranKhan on 28-07-2019.
  */
object dataSourceAPIs {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
    val spark = SparkSession.builder().master("local").config(conf = conf).appName("DSLoperation2").getOrCreate()
    val sc = spark.sparkContext
    sc.setLogLevel("ERROR")

    //val data = spark.read.csv("D:\\Spark\\data-master\\data-master\\nyse\\nyse_2009.csv")


    val dataDF = spark.read.format("com.databricks.spark.csv")
      .option("delimiter", ",")
      .option("header", true)
      .option("inferSchema", true).option("escape", "\"").option("quote","""""")
      .load("D:\\Spark\\data-master\\data-master\\data.csv") //.load("D:\\Spark\\data-master\\data-master\\nyse\\nyse_2009.csv")
    dataDF.show()
    dataDF.printSchema()
    val jsondata = spark.read.json("D:\\Spark\\data-master\\data-master\\retail_db_json\\orders\\part-r-00000-990f5773-9005-49ba-b670-631286032674")
    jsondata.show()
    jsondata.printSchema()
    val parqetdata = spark.read.parquet("D:\\Spark\\data-master\\data-master\\userdata1.parquet")
    parqetdata.show()
    parqetdata.printSchema()





  }

}
