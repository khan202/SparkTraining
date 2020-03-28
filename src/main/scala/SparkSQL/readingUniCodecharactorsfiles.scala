package SparkSQL

import org.apache.spark.SparkConf
import org.apache.spark.sql.{SaveMode, SparkSession}

object readingUniCodecharactorsfiles {

  def main(args: Array[String]): Unit = {

    val conf = new SparkConf()
    val spark = SparkSession.builder().master("local").config(conf = conf).appName("customerDF").getOrCreate()
    val sc = spark.sparkContext
    sc.setLogLevel("ERROR")

    val df = spark.read
      .format("com.databricks.spark.csv")
      .option("header", "true")
      .option("inferSchema", "true")
      .option("delimiter", ",")
      .option("encoding", "ISO-8859-1")
      .csv("C:/ProgramData/MySQL/MySQL Server 8.0/Uploads/Customers.csv")
    df.show()

    df.write
      .option("header", "true").mode("overwrite").format("csv")
      .option("encoding", "UTF-8").save("D:\\BigData\\data-master\\Customers1.csv")
    val url = "jdbc:mysql://localhost:3306/azure"
    val prop = new java.util.Properties()
    prop.put("user", "root")
    prop.put("password", "root")


    df.write.mode(SaveMode.Overwrite).jdbc(url, "customers1", prop)
  }


}
