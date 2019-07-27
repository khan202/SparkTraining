package SparkSQL

import org.apache.spark.SparkConf
import org.apache.spark.sql._
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._

/**
  * Created by ImranKhan on 27-07-2019.
  */
object DFjoins {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
    val spark = SparkSession.builder().master("local").config(conf = conf).appName("DSLoperation2").getOrCreate()
    val sc = spark.sparkContext
    sc.setLogLevel("ERROR")
    val s = "order_id:Int,order_date:String,order_customer_id:Int,order_status:String"
    val fields = s.split(",").map(e => {
      val a = e.split(":")
      val f = a(0)
      val t = if (a(1) == "Int") IntegerType else StringType
      StructField(f, t, nullable = false)
    })
    val orderschema = StructType(fields)

    val orderdata = sc.textFile("D:\\Spark\\data-master\\data-master\\retail_db\\orders\\part-00000")
    orderdata.take(2).foreach(println)

    val ordersRDD = orderdata.map(e => {
      val w = e.split(",")
      Row(w(0).toInt, w(1), w(2).toInt, w(3))
    })
import spark.implicits._
    val ordersDF = spark.createDataFrame(ordersRDD, orderschema)
    // ordersDF.show(10)
    val orders = sc.textFile("D:\\Spark\\data-master\\data-master\\retail_db\\order_items\\part-00000")
    orders.take(2).foreach(println)
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
    //orderItems.show()

    ordersDF.join(orderItems, $"order_id"===$"order_item_order_id").show()
    ordersDF.join(orderItems, $"order_id"===$"order_item_order_id", "left").where($"order_item_order_id".isNull).show()
  }

}
