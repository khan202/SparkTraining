package SparkRDD

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

/**
 * Created by ImranKhan on 13-07-2019.
 */
object aggregateByKeyTransformation {
  def main(args: Array[String]): Unit = {

    val conf = new SparkConf()
    val spark = SparkSession.builder().master("local").config(conf = conf).appName("aggregateByKeyTransformation").getOrCreate()
    val sc = spark.sparkContext
    val order_items = sc.textFile("D:\\BigData\\data-master\\retail_db\\order_items\\part-00000")
    val orderItemmap = order_items.map(e => (e.split(",")(1).toInt, e.split(",")(4).toFloat))
    orderItemmap.foreach(println)
  val revenueAndCountPerOrder = orderItemmap.aggregateByKey((0.0f, 0))((a1: (Float, Int), a2: (Float)) =>
      (a1._1 + a2, a1._2 + 1), (f1: (Float, Int), f2: (Float, Int)) => (f1._1 + f2._1, f1._2 + f2._2)) //.take(10).foreach(println)
//
    revenueAndCountPerOrder.take(10).foreach(println)
    //revenueAndCountPerOrder.saveAsTextFile("D:\\Spark\\results\\orderItems1")
  }

}
