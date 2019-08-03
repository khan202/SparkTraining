package SparkSQL

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types._
import org.apache.spark.sql._
import org.apache.spark.sql.functions._

object customerDF {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
    val spark = SparkSession.builder().master("local").config(conf = conf).appName("customerDF").getOrCreate()
    val sc = spark.sparkContext
    sc.setLogLevel("ERROR")

    val customerData = sc.textFile("G:\\Spark\\data-master\\retail_db\\customers\\part-00000")
    //customerData.take(10).foreach(println)
    //1,Richard,Hernandez,XXXXXXXXX,XXXXXXXXX,6303 Heather Plaza,Brownsville,TX,78521

    val customer = customerData.map(e => {
      val w = e.split(",")
      Row(w(0).toInt,w(1),w(2),w(3),w(4),w(5),w(6),w(7),w(8).toInt)
    })
    //customer.take(10).foreach(println)

    val schema = StructType(
      List(
        StructField("customer_id", IntegerType, false),
        StructField("customer_fname", StringType, false),
        StructField("customer_lname", StringType, false),
        StructField("customer_email", StringType, false),
        StructField("customer_password", StringType, false),
        StructField("customer_street", StringType, false),
        StructField("customer_city", StringType, false),
        StructField("customer_state", StringType, false),
        StructField("customer_Zipcode", IntegerType, false)
      )
    )
    import spark.implicits._
    val dataframe = spark.createDataFrame(customer, schema)

    dataframe.createTempView("customers")
    dataframe.show()
    dataframe.select($"customer_id",length($"customer_id").alias("customer_id_count")).show()
    //dataframe.select(lit("customer_fname")).show()
  }

}
