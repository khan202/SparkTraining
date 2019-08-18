package SparkRDD

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

/**
  * Created by ImranKhan on 17-08-2019.
  */
object wordcount{

  def main(args: Array[String]): Unit = {

    val conf = new SparkConf()
    val spark = SparkSession.builder().master(args(0)).config(conf = conf).appName("wordcount").getOrCreate()

    val sc = spark.sparkContext

    val lines = sc.textFile(args(1))
    //List("Hello World", "How are you", "are you interested", "in writing", " simple word count program", "using spark", "to demonstrate", "flatMap")
    //val rdd = sc.parallelize(lines)

    val rddmap = lines.flatMap(e => e.split(" ")).map(e => (e, 1)).reduceByKey(_ + _)

    //rddmap.foreach(println)
    //rddmap.saveAsTextFile(args(2))
    println(args(2))



  }
}
