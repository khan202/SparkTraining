package DataSet

import org.apache.spark.sql.SparkSession

object createDataset {
  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder().master("local").appName("Create DataSets").getOrCreate()
    val data = spark.read.csv("D:\\BigData\\data-master\\retail_db\\orders\\orders\\orders.csv").as("orders")
    data.show()


    spark.stop()


  }
}
