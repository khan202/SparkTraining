package SparkSQL

//import org.apache.avro.generic.GenericData.StringType
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql._
import org.apache.spark.sql.types._


/**
  * Created by ImranKhan on 21-07-2019.
  */
object createDFProgrametically {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
    val spark = SparkSession.builder().master("local").config(conf = conf).appName("createDFProgrametically").getOrCreate()
    val sc = spark.sparkContext
    sc.setLogLevel("ERROR")
    val s = "order_Id:Int, order_date:String, order_customer_Id:Int, order_status:String"
    val fields = s.split(",").map(e => {
      val a = e.split(":")
      val f = a(0)
      val t = if (a(1) == "Int") IntegerType else StringType
      StructField(f, t, nullable = false)
    })
    val schema = StructType(fields)

    val orders = sc.textFile("D:\\Spark\\data-master\\data-master\\retail_db\\orders\\part-00000")
    orders.take(2).foreach(println)
    /*1,2013-07-25 00:00:00.0,11599,CLOSED
    2,2013-07-25 00:00:00.0,256,PENDING_PAYMENT*/
    val ordersRDD = orders.map(e => {
      val w = e.split(",")
      Row(w(0).toInt, w(1), w(2).toInt, w(3))
    })


    val ordersDF = spark.createDataFrame(ordersRDD, schema)
    //ordersDF.show(10)
    ordersDF.select("order_Id", "order_date")

    //String manipulations
    //val dual = Seq("X").toDF("dummy").show()


  }

}
