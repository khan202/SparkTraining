package OracleSQLPractice

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._


object WhereClause {
  def main(args: Array[String]): Unit = {

    val conf = new SparkConf()
    val spark = SparkSession.builder().master("local").appName("WhereClause").config(conf = conf).getOrCreate()

    val data = spark.read.option("header", false).option("inferschema", true).option("delimeter", ",").format("csv").load("D:\\BigData\\data-master\\retail_db\\orders\\part-00000")
    val orders = data.toDF("order_id", "order_date", "order_total", "order_status").persist()

    data.write.format("csv").save("D:\\BigData\\data-master\\retail_db\\orders\\orders.csv")//orders.show()
   /* import spark.implicits._
    orders.select("order_id", "order_status").where($"order_status" === "PENDING_PAYMENT")
    orders.select("order_id", "order_status").filter($"order_status" === "PENDING_PAYMENT")

    val items = List(1, 2, 3)
    orders.filter($"order_id".isin(items: _*))
    //or
    orders.filter($"order_id".isin(1, 2, 3)).show

    orders.select("order_id", "order_date", "order_status").where("order_date LIKE '2013-07%'").alias("orderDate").groupBy("order_date").agg(count("order_id").alias("totalorders")).show()

*/
  }
}