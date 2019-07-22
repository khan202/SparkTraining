package SparkSQL

import org.apache.spark.SparkConf
import org.apache.spark.sql._
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._


/**
  * Created by ImranKhan on 21-07-2019.
  */
object DSLOperation {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
    val spark = SparkSession.builder().master("local").config(conf = conf).appName("createDFProgrametically").getOrCreate()
    val sc = spark.sparkContext
    sc.setLogLevel("ERROR")
    val s = "order_id:Int,order_date:String,order_customer_id:Int,order_status:String"
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
    // ordersDF.show(10)


    //ordersDF.select(
    ordersDF.select("order_id", "order_date").show()
    //ordersDF.select()
    import spark.implicits._
    val dual = Seq("X").toDF("dummy") //.show()
    dual.select($"dummy").show()
    /*dual.select(lit("Hello World")).show()
    dual.select(lit("Hello World").alias("dummy")).show()
    dual.select(length(lit("Hello World")).alias("dummy")).show()
    dual.select(lower(lit("Hello World")).alias("dummy")).show()
    dual.select(substring(lit("Hello World"), 1, 5).alias("dummy")).show()
    dual.select(instr(lit("Hello World"), " ").alias("dummy")).show()
    dual.select(trim(lit("Hello World   ")).alias("dummy")).show()
    dual.select(trim(lit("Hello World000000"), "0")).alias("dummy").show()
    dual.select(lpad(lit("7"), 3, "0").alias("dummy")).show()
    dual.select(concat(lit("hello"), lit(" "), lit("world")).alias("dummy")).show()
    dual.select(initcap(lit("hello world").alias("dummy"))).show()
    dual.select(translate(lit("hello world"), " ", "_")).show()
    dual.select(regexp_replace(lit("2019-04-14"), "-", "/")).show()

    //date manipulations

    dual.select(current_date().alias("dummy")).show()
    dual.select(current_timestamp().alias("dummy")).first()

    dual.select(date_add(current_date(), 7).alias("dummy")).show()
    dual.select(date_sub(current_date(), 7).alias("dummy")).show()
    dual.select(datediff(lit("2019-04-14"), lit("2019-06-25")).alias("dummy")).show()
    dual.select(date_format(current_timestamp(), "dd").alias("dummy")).show()
    dual.select(date_format(current_timestamp(), "dd-MM-yyyy HH:mm:SS").alias("dummy")).show()


    //ordersDF.select($"order_date,date_format"($"order_date","YYYYMMdd").cast(IntegerType).alias("order_date"),$"order_customer_id",$"order_status").show()//.printSchema()

    ordersDF.where("order_status='COMPLETE' or order_status = 'CLOSED'").show()*/
    ordersDF.where("order_status IN ('COMPLETE','CLOSED')").show()
    ordersDF.where("order_status IN ('COMPLETE','CLOSED') AND order_date LIKE '2014-08%'").show()
    ordersDF.where(col("order_status") === "COMPLETE" || col("order_status") === "CLOSED").show()
    ordersDF.where(col("order_status").isin("COMPLETE", "CLOSED") && (col("order_date").like("2013-08%"))).show()




  }

}
