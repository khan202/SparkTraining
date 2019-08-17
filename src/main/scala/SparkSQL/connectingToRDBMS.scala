package SparkSQL

import org.apache.spark.SparkConf
import org.apache.spark.sql.{SaveMode, SparkSession}

/**
  * Created by ImranKhan on 04-08-2019.
  */
object connectingToRDBMS {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
    val spark = SparkSession.builder().master("local").config(conf = conf).appName("connectingToRDBMS").getOrCreate()
    val sc = spark.sparkContext
    sc.setLogLevel("ERROR")

    val prop = new java.util.Properties()
    prop.put("user", "root")
    prop.put("password", "root")
    val url = "jdbc:mysql://localhost:3306/retail_db"

    val df = spark.read.jdbc(url, "customers", prop)
    //spark.read.jdbc(connection url, tableName, username and password)
    df.show()

    //df.write.mode(SaveMode.Append).jdbc(url, "customers1", prop)
    df.write.mode(SaveMode.Overwrite).jdbc(url, "customers1", prop)
  }

}
