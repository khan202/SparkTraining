package SparkRDD

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

/**
  * Created by ImranKhan on 07-07-2019.
  */
object filterTransformation {
  def main(args: Array[String]): Unit = {
    val config = new SparkConf()
    val spark = SparkSession.builder().master("local").appName("flatTransformation").getOrCreate()
    val sc = spark.sparkContext
    val orders = sc.textFile("D:\\Spark\\data-master\\data-master\\retail_db\\orders\\part-00000",2)
    val complteOrders = orders.filter(e => e.split(",")(3)=="COMPLETE" || e.split(",")(3) == "CLOSED")
    print("****************************************"+ complteOrders.count()+ "************************")
    complteOrders.take(10).foreach(println)

    val orders201307 = orders.filter( e => { e.split(",")(1).startsWith("2013-07") })
    print("******************************************************************************")
    print("************"+ orders.filter( e => { e.split(",")(1).startsWith("2013-07") && (e.split(",")(3) =="CLOSED" || e.split(",")(3)=="COMPLETED")}
    ).count() + "**************************")



  }

}
