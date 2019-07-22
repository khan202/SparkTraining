package SparkRDD

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

/**
  * Created by ImranKhan on 14-07-2019.
  */
object sortByKey {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
    val spark = SparkSession.builder().master("local").config(conf = conf).appName("sortByKey").getOrCreate()
    val sc = spark.sparkContext

    val order_items = sc.textFile("D:\\Spark\\data-master\\data-master\\retail_db\\order_items\\part-00000")
    val orderItemsmap = order_items.map(e => (e.split(",")(1).toInt, e.split(",")(4).toFloat))
    val orderItemsGroupByKey = orderItemsmap.groupByKey(3).map(e=>(e._1,e._2.sum)).sortBy(e => -e._2)//.filter(e=> e._1 == 57436)//.collect().foreach(println)
    orderItemsGroupByKey.take(10).foreach(println)
    //val revenueperorder = orderItemsGroupByKey.sortBy(e=> -e._2) //.take(5).foreach(println)
   // val OrderRevenuesorted = revenueperorder.map(e => (e._2, e._1)).sortByKey(false) //.take(10).foreach(println)
    //val OrderRevenuesortedwithorderId = OrderRevenuesorted.map(e => (e._2, e._1)).take(10).foreach(println)
    // val OrderRevenuesorted = orderItemsGroupByKey.map( e=> (e._1,e._2.toList.sortBy(r=> -r))).take(5).foreach(println)
    // val OrderRevenuesorted1= revenueperorder.sortByKey()
    //.take(5).foreach(println)
//order_items.mapPartitionWithIndex
  }
}
