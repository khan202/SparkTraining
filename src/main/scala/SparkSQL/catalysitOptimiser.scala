package SparkSQL

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

/**
  * Created by ImranKhan on 28-07-2019.
  */
object catalysitOptimiser {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
    val spark = SparkSession.builder().master("local").config(conf = conf).appName("catalysitOptimiser").getOrCreate()
    val sc = spark.sparkContext
    sc.setLogLevel("ERROR")
    /* val userData = spark.read.option("header", true).option("delimiter", ",").option("inferschema", true).csv("D:\\Spark\\data-master\\data-master\\user.csv")
     userData.show()

     val purchaseData = spark.read.option("header", true).option("delimiter", ",").option("inferschema", true).csv("D:\\Spark\\data-master\\data-master\\purchase.csv")
     purchaseData.show()

     val joinedopt = purchaseData.join(userData, Seq("userid"), "left").select("pid", "location").filter("amount>=60").select("location") //.show()
     joinedopt.explain(true)*/
    val empdata = spark.read.option("header", true).option("delimiter", ",").option("inferschema", true).csv("D:\\Spark\\employee.csv")

    empdata.show()
    val deptdata = spark.read.option("header", true).option("delimiter", ",").option("inferschema", true).csv("D:\\Spark\\dept.csv")
    deptdata.show()
    spark.conf.set("spark.sql.shuffle.partitions", 20) //to cntrl the run time shuffle partitions during the join condition

    spark.conf.set("spark.sql.autoBroadcastJoinThreshold", -1) // to explicitly disabling the broadcastJoin
    val joindf = empdata.join(deptdata, "dno").filter("salary>20000")
    joindf.show()
    val result = joindf.select("ename", "gender", "salary", "location")

    result.show()
    result.explain(true)


  }

}
