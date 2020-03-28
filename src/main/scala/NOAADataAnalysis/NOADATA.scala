package NOAADataAnalysis

import org.apache.spark.sql.SparkSession

object NOADATA {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().master("local[*]").appName("NOA Data").getOrCreate()
    val data2017= spark.read.option("header", true).option("inferschema", true).option("dateFormat","yyyyMMdd").csv("D:\\BigData\\data-master\\NOAA\\TX417945_8515.csv")
    data2017.show()
    println(data2017.schema)


  }

}
