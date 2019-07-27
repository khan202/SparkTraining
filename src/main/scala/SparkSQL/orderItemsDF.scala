package SparkSQL

import org.apache.spark.SparkConf
import org.apache.spark.sql.types.{FloatType, IntegerType, StructField, StructType}
import org.apache.spark.sql._

//import org.apache.spark.sql.functions._

/**
  * Created by ImranKhan on 27-07-2019.
  */
object orderItemsDF {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
    val spark = SparkSession.builder().master("local").config(conf = conf).appName("DSLoperation2").getOrCreate()
    val sc = spark.sparkContext
    //sc.setLogLevel("ERROR")
    val orders = sc.textFile("D:\\Spark\\data-master\\data-master\\retail_db\\order_items\\part-00000")
    //orders.take(2).foreach(println)
    /*1,2013-07-25 00:00:00.0,11599,CLOSED
    2,2013-07-25 00:00:00.0,256,PENDING_PAYMENT*/
    val orderItemsRDD = orders.map(e => {
      val w = e.split(",")
      Row(w(0).toInt, w(1).toInt, w(2).toInt, w(3).toInt, w(4).toFloat, w(5).toFloat)
    })
    val schema = StructType(
      List(
        StructField("order_item_id", IntegerType, false),
        StructField("order_item_order_id", IntegerType, false),
        StructField("order_item_product_id", IntegerType, false),
        StructField("order_item_quantity", IntegerType, false),
        StructField("order_item_subtotal", FloatType, false),
        StructField("order_item_product_price", FloatType, false)
      )
    )
    //import spark.implicits._
    val orderItems = spark.createDataFrame(orderItemsRDD, schema)
    orderItems.show()

  }

}
