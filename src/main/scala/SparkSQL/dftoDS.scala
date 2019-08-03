package SparkSQL

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

/**
  * Created by ImranKhan on 03-08-2019.
  */


object dftoDS {

  case class order(order_id: Int, order_date: String, order_customer_id: Int, order_status: String)


  def main(args: Array[String]): Unit = {

    val conf = new SparkConf()
    val spark = SparkSession.builder().master("local").config(conf = conf).appName("dftoDS").getOrCreate()
    val sc = spark.sparkContext
    sc.setLogLevel("ERROR")
    import spark.implicits._

    val orders = sc.textFile("D:\\Spark\\data-master\\data-master\\retail_db\\orders\\part-00000")
    val ordersDF = orders.map(e => {
      val a = e.split(",")
      order(a(0).toInt, a(1), a(2).toInt, a(3))
    }).toDF()

    ordersDF.show()
    val ordersDS = ordersDF.as[order]
    ordersDS.show()
    ordersDS.filter(e => e.order_status == "CLOSED").show()
  }

}


