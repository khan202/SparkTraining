package SparkSQL

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

object AddingDynamicColumns {

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
    val spark = SparkSession.builder().master("local[]").appName("AddingDynamicColumns").config(conf = conf).getOrCreate()


  }

}
