package SparkSQL

import org.apache.spark.SparkConf
import org.apache.spark.sql.types.{FloatType, IntegerType, StructField, StructType}
import org.apache.spark.sql.{Row, SparkSession}

/**
 * Created by ImranKhan on 17-08-2019.
 */
object sparkHiveIntegration {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
    val spark = SparkSession.builder().master("local").config(conf = conf).appName("sparkHiveIntegration").enableHiveSupport().getOrCreate()
    val sc = spark.sparkContext
    sc.setLogLevel("ERROR")

    val orders = sc.textFile("D:\\Spark\\data-master\\data-master\\retail_db\\order_items\\part-00000")
    spark.sql("create database if not exists imrankhan")

    spark.catalog.listDatabases().show()
    spark.catalog.setCurrentDatabase("imrankhan")
    spark.sql("create external table if not exists imrankhan.employee(eno int, ename string, sal int, gender string, dno int) location '/'")
    spark.sql("LOAD DATA LOCAL INPATH 'D:\\Spark\\employee.txt' OVERWRITE INTO TABLE employee")
    spark.sql("INSERT OVERWRITE TABLE employee values(101,'abc',10000,'male',11)")
    spark.catalog.listTables("imrankhan").show()


  }

}
