package SparkRDD

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

/**
  * Created by ImranKhan on 13-07-2019.
  */
object groupByKeyTransformation {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
    val spark = SparkSession.builder().master("local").config(conf = conf).appName("groupByKeyTransformation").getOrCreate()
    val sc = spark.sparkContext
    //val data = sc.textFile("file:///home/cloudera/Desktop/employee.txt")
    val data = sc.textFile("D:\\Spark\\employee.txt")
    val aar = data.map(e => e.split(","))
    val  pair1 = aar.map(e => (e(4), e(2).toInt))
    val grp = pair1.groupByKey()
    grp.collect.foreach(println)
    val res = grp.map { e =>
      val dno = e._1
      val cb = e._2
      val tot = cb.sum
      val cnt = cb.size
      val avg = tot / cnt
      val max = cb.max
      val min = cb.min
      //(dno, tot, cnt, avg, max, min)
      //or
      val r = dno+","+tot+","+cnt+","+avg+","+max+","+min
      r
    }
    res.collect.foreach(println)




    }





}
