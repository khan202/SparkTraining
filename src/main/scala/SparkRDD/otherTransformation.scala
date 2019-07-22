package SparkRDD

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

/**
  * Created by ImranKhan on 14-07-2019.
  */
object otherTransformation {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
    val spark = SparkSession.builder().master("local").config(conf = conf).appName("otherTransformation").getOrCreate()
    val sc = spark.sparkContext
    val l = List(1,2,3,4,5,6)
    val m = List('a','b','c','d','e','f')

    val l1 = sc.parallelize(l)
    val m1 = sc.parallelize(m)

    val k = l1.cartesian(m1)
    k.collect.foreach(println)
    val n = k.cogroup(k)
    n.collect.foreach(println)

  }

}
