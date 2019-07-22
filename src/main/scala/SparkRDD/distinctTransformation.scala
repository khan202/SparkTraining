package SparkRDD

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

/**
  * Created by ImranKhan on 14-07-2019.
  */
object distinctTransformation {
  def main(args: Array[String]): Unit = {


    val conf = new SparkConf()
    val spark = SparkSession.builder().master("local").config(conf = conf).appName("distinctTransformation").getOrCreate()
    val sc = spark.sparkContext

    // val data = sc.textFile("D:\\Spark\\data-master\\data-master\\nyse\\nyse_2009.csv")
    val l = List(1, 1, 2, 2, 3, 3, 4, 5, 6, 7, 8, 8, 8, 9, 9, 9, 4, 4, 5, 6)
    val m = List(1, 1, 2, 2, 3, 3, 4, 5, 6, 7, 8, 8, 8, 9, 9, 9, 4, 4, 5, 6)
    val j = l ++ m // print(l)

    val data = sc.parallelize(j, 4)
    val data1 = sc.parallelize(m)
    val result = data.intersection(data1)
    println("data intersection")
    result.collect.foreach(print)
    println("Number of partitions before repartition")
    println(data.partitions.size)
    println("Number of partitions after repartition")
    println(data.repartition(2).partitions.size)
    println("Number of partitions after coalesce")
    println(data.coalesce(3, false).partitions.size)

    /*  val ldist = data.distinct()
      ldist.collect.foreach(print)
      //ldist.coalesce()*/
  }

}
