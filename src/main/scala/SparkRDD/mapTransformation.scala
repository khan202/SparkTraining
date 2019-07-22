package SparkRDD
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession


object mapTransformation {
  def main(args: Array[String]): Unit = {
    System.setProperty("hadoop.home.dir", "D:\\Spark\\Hadoop")
    val sparkConf = new SparkConf()//.set("spark.driver")
    val spark = SparkSession.builder().master("local").config(conf = sparkConf).appName("mapTransformation").getOrCreate()
    val sc = spark.sparkContext
    val orders = sc.textFile("D:\\Spark\\data-master\\data-master\\retail_db\\orders\\part-00000", 1)
//    orders.first().foreach(print)
//    orders.take(10).foreach(println)
 /*   val l = List(1,2,3,4,5)
    val data = sc.parallelize(l)
    data.collect().foreach(println)

*/ val orderDates = orders.map(e => e.split(",")(1))
   // orderDates.take(10).foreach(println)
    print("**********************orders count***************")
    println(orders.count())
    print("**********************ordersDates count***************")
    println(orderDates.count())
    val orderDate_YYYYMMDD = orders.map(e => e.split(",")(1).substring(0,10).replace("-","").toInt)
    orderDate_YYYYMMDD.take(10).foreach(println)
    val orderDates_pair = orders.map(e=>(e.split(",")(1).substring(0,10).
      replace("-","").toInt,1)).reduceByKey(_+_)
    print("*********************************reduceByKey********************")
    orderDates_pair.take(10).foreach(println)

  }
}
