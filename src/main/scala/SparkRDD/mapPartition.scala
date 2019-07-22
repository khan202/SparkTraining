package SparkRDD

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

/**
  * Created by ImranKhan on 14-07-2019.
  */
object mapPartition {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
    val spark = SparkSession.builder().master("local").config(conf = conf).appName("mapPartition").getOrCreate()
    val sc = spark.sparkContext
    val data = sc.textFile("D:\\Spark\\data-master\\data-master\\nyse\\nyse_2009.csv")
    data.take(10).foreach(println) /*A,01-Jan-2009,15.63,15.63,15.63,15.63,0*/

    val datamap = data.map { e =>
      val a = e.split(",")
      ((a(1), a(0)), a(6).toLong)
    }
    datamap.take(5).foreach(println) /* ((01-Jan-2009,A),0)*/


    val mapPartitionData = data.mapPartitions(e => {
      e.map(nyscrec => {
        val a = nyscrec.split(",")
        ((a(1), a(0)), a(6).toLong)
      })
    })
    print("******************************************************************")
    println()
    println(mapPartitionData.toDebugString)
    mapPartitionData.take(10).foreach(println) /*((01-Jan-2009,A),0)*/


  }


}
