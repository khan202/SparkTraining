package SparkSQL

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

/**
  * Created by ImranKhan on 20-07-2019.
  */
object ToCreateDFFromRDD {

  case class Employee(eno: Int, ename: String, sal: Int, gen: String, dno: Int)

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
    val spark = SparkSession.builder().master("local").config(conf = conf).appName("ToCreateDF").getOrCreate()
    val sc = spark.sparkContext
    sc.setLogLevel("ERROR")
    val empdata = sc.textFile("D:\\Spark\\employee.txt")
    // empdata.take(10).foreach(println)
    val emap = empdata.map(x => {
      val w = x.split(",")
      Employee(w(0).toInt, w(1), w(2).toInt, w(3), w(4).toInt)
    })
    import spark.implicits._

    val edf = emap.toDF()
    edf.show()
    edf.printSchema()//to print the schema

    //edf.registerTempTable("employee")
    edf.createOrReplaceTempView("employee")
    //edf.createGlobalTempView("employee1")
    //edf.createOrReplaceGlobalTempView("employee")

    val res = spark.sql("select * from employee")

    res.show()

  }

}
