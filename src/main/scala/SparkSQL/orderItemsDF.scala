package SparkSQL

import com.mysql.cj.x.protobuf.MysqlxDatatypes.Scalar
import org.apache.spark.SparkConf
import org.apache.spark.sql.types.{FloatType, IntegerType, StructField, StructType}
import org.apache.spark.sql._

import scala.collection.immutable.Range.Int

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
    val orders = sc.textFile("D:\\BigData\\data-master\\retail_db\\order_items\\part-00000")
    //orders.take(2).foreach(println)
    /*1,2013-07-25 00:00:00.0,11599,CLOSED
    2,2013-07-25 00:00:00.0,256,PENDING_PAYMENT*/
    val orderItemsRDD = orders.map(e => {
      val w = e.split(",")
      Row(w(0).toInt, w(1).toInt, w(2).toInt, w(3).toInt, w(4).toFloat, w(5).toFloat)
    })
    orderItemsRDD.foreach(println)// prints tuple of rows
        /*(141687,56652,1004,1,399.98,399.98)
    (141688,56652,191,2,199.98,99.99)
    (141689,56653,957,1,299.98,299.98)
    */
    //val orderItemstuple = orderItemsRDD.map(e=>((e._1),(e._2,e._3,e._4,e._5)))
   // orderItemstuple.foreach(println)// prints the tuple of tuple eg:(key,(value1, value2,....))
   /* (171663,(68664,403,1,129.99))
    (171664,(68664,1004,1,399.98))
    df.take(5)
       */
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
    import spark.implicits._
    val orderItems = spark.createDataFrame(orderItemsRDD, schema)
   orderItems.show()


  }

}
