package SparkSQL

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan

/**
  * Created by ImranKhan on 03-08-2019.
  */
object costomisedOptimization {
  def main(args: Array[String]): Unit = {

    val conf = new SparkConf()
    val spark = SparkSession.builder().master("local").config(conf = conf).appName("catalysitOptimiser").getOrCreate()
    val sc = spark.sparkContext
    sc.setLogLevel("ERROR")

    val empdata = spark.read.option("header", true).option("delimiter", ",").option("inferschema", true).csv("D:\\Spark\\employee.csv")
    //    val data = spark.read.option("text").op

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


/*
def MultiplyOptimizationRule extends Rule[LogicalPlan]{





}*/
