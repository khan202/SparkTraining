package SparkSQL

import org.apache.spark.SparkConf
import org.apache.spark.sql.{types, _}
import org.apache.spark.sql.types.{FloatType, IntegerType, StructField, StructType}
//import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._


object CreatingDFusingSFST {
  def main(args: Array[String]): Unit = {

    val conf = new SparkConf()
    val spark = SparkSession.builder().master("local").config(conf = conf).appName("CreatingDFusingSFST").getOrCreate()
    val sc = spark.sparkContext
    sc.setLogLevel("ERROR")
    val orders = sc.textFile("G:\\Spark\\data-master\\retail_db\\order_items\\part-00000")
    /* orders.take(10).foreach(println)
     1,1,957,1,299.98,299.98
     2,2,1073,1,199.99,199.99*/

    val ordersdata = orders.map(e => {
      val w = e.split(",")
      Row(w(0).toInt, w(1).toInt, w(2).toInt, w(3).toInt, w(4).toFloat, w(5).toFloat)

    })
    ordersdata.take(10).foreach(println)
    val schema = StructType(
      List(
        StructField("order_item_id", IntegerType, false),
        StructField("order_item_order_id", IntegerType, true),
        StructField("order_item_product_id", IntegerType, true),
        StructField("order_item_quantity", IntegerType, true),
        StructField("order_item_subtotal", FloatType, true),
        StructField("order_item_product_price", FloatType, true)
      )
    )
    val orderItemsDF = spark.createDataFrame(ordersdata, schema)
    orderItemsDF.createTempView("order_items")
    //orderItemsDF.select("*").show()
    import spark.implicits._
    //orderItemsDF.select("*").show()
    orderItemsDF.select(max("order_item_quantity")).show()
    orderItemsDF.select($"order_item_order_id", sum($"order_item_subtotal")).alias("revenue_per_order").show()
  }
}
