package SparkRDD
import org.apache.spark.sql.SparkSession
import org.apache.spark.SparkConf
/**
  * Created by ImranKhan on 07-07-2019.
  */
object flatMapTransformation {
  def main(args: Array[String]): Unit = {
    val config = new SparkConf()
    val spark = SparkSession.builder().master("local").appName("flatTransformation").getOrCreate()
    val sc = spark.sparkContext
    val lines = List("Hello World","How are you","are you interested","in writing"," simple word count program","using spark","to demonstrate","flatMap")
    val linesdRDD = sc.parallelize(lines,4)
    print(linesdRDD.partitions.size)
    linesdRDD.repartition(2)
    print(linesdRDD.partitions.size)
    /*val res = linesdRDD.flatMap(e => e.split(" ")).map(e => (e,1)).reduceByKey(_+_)
    print("*********************************************************************")
    println()
    print(res.toDebugString)

    res.collect().foreach(println)*/

  }
}
