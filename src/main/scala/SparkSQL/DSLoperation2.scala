package SparkSQL

import org.apache.spark.SparkConf
import org.apache.spark.sql._
import org.apache.spark.sql.types._
//import org.apache.spark.sql.functions._

/**
  * Created by ImranKhan on 21-07-2019.
  */
object DSLoperation2 {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
    val spark = SparkSession.builder().master("local").config(conf=conf).appName("DSLoperation2").getOrCreate()
    val sc = spark.sparkContext

    val s = "order_id:Int, order_date:String, order_customer_id:Int, order_status:String"
    val fields = s.split(",").map(e => {
      val a = e.split(":")
      val f = a(0)
      val t = if (a(1) == "Int") IntegerType else StringType
      StructField(f, t, nullable = false)
    })
    val schema = StructType(fields)

    val orders = sc.textFile("D:\\Spark\\data-master\\data-master\\retail_db\\orders\\part-00000")
    orders.take(2).foreach(println)
    val ordersRDD = orders.map(e => {
      val w = e.split(",")
      Row(w(0).toInt, w(1), w(2).toInt, w(3))
    })

    val ordersDF = spark.createDataFrame(ordersRDD, schema)
    ordersDF.select("order_Id", "order_date").show()




  }

}
