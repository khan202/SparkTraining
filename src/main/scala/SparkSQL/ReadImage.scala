package SparkSQL

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

object ReadImage {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
    val spark = SparkSession.builder().master("local").config(conf=conf).appName("ReagImage").getOrCreate()

    //spark.read.format("image").load("C:\\Users\\imran\\Desktop\\Adhaar - Imrankhan.png").show()
   spark.read.format("image").load("C:\\Users\\imran\\Desktop\\Adhaar - Imrankhan.png").show()


  }

}
