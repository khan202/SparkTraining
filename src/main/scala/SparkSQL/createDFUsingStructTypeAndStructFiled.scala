package SparkSQL

import org.apache.spark.SparkConf
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}
import org.apache.spark.sql.{Row, SparkSession}

/**
  * Created by ImranKhan on 21-07-2019.
  */
object createDFUsingStructTypeAndStructFiled {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
    val spark = SparkSession.builder().master("local").config(conf = conf).appName("createDFUsingStructTypeAndStructFiled").getOrCreate()
    val sc = spark.sparkContext

    sc.setLogLevel("ERROR")
    val data = Seq(Row(1, "a"), Row(5, "z"))
    val schema = StructType(
      List(
        StructField("num", IntegerType, true),
        StructField("letter", StringType, true)
      )
    )


   // val orders = sc.textFile("D:\\Spark\\data-master\\data-master\\retail_db\\orders\\part-00000")
    //orders.take(2).foreach(println)
    /*1,2013-07-25 00:00:00.0,11599,CLOSED
    2,2013-07-25 00:00:00.0,256,PENDING_PAYMENT*/
    /*val ordersRDD = orders.map(e => {
      val w = e.split(",")
      Row(w(0).toInt, w(1), w(2).toInt, w(3))
    })*/

    val ordersDF = spark.createDataFrame(spark.sparkContext.parallelize(data), schema)
    ordersDF.show(10)

    print(spark.catalog.listDatabases())


  }

}
