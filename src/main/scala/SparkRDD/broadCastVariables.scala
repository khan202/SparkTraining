package SparkRDD

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

/**
  * Created by ImranKhan on 20-07-2019.
  */
object broadCastVariables {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
    val spark = SparkSession.builder().master("local").config(conf = conf).appName("broadCastVariables").getOrCreate()
    val sc = spark.sparkContext

    val data = "this is spark programmoing and we can write spark in scala".split(" ")
    val lkp = Map("spark" -> 1, "hadoop" -> 2)
    val rdd1 = sc.parallelize(data)
    val bc = sc.broadcast(lkp)
    val res = rdd1.map(x => bc.value.getOrElse(x, 0))
    res.collect.foreach(print)


  }

}
