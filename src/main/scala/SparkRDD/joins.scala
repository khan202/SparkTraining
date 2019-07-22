package SparkRDD

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

/**
  * Created by ImranKhan on 13-07-2019.
  */
object joins {
  def main(args: Array[String]): Unit = {

    val config = new SparkConf()
    val spark = SparkSession.builder().master("local").appName("joins").getOrCreate()
    val sc = spark.sparkContext

//    val data1 = List((1,"abc"),(2,"def"),(3,"klm"),(4,"pqr"))
//    val data2 = List((1,10),(2,11),(3,12))
//    val data1RDD = sc.parallelize(data1)
//    val data2RDD = sc.parallelize(data2)
//    val result = data1RDD.join(data2RDD)
//    result.collect().foreach(println)

    val empData = sc.textFile("D:\\Spark\\employee.txt")
    val deptData = sc.textFile("D:\\Spark\\dept.txt")
    val edata = empData.map({e => val w =  e.split(",")
      val eno = w(0).toInt
      val ename = w(1)
      val sal = w(2).toInt
      val gender = w(3)
      val dno = w(4).toInt
      (dno,(eno,ename,sal,gender))
       })
    val ddata = deptData.map({e =>
      val w = e.split(",")
      val dno = w(0).toInt
      val dname = w(1)
      val loc = w(2)
      (dno,(dname,loc))
      })
    val result = edata.join(ddata)
    result.collect.foreach(println) //(13,((104,dddd,40000,m),(HR,Delhi)))

    val e = result.map(e => (e._2._1._1,e._2._1._2,e._2._1._3,e._2._1._4,e._2._2._1,e._2._1._2,e._1))

    e.collect.foreach(println)//(104,dddd,40000,m,HR,dddd,13)

   // e.saveAsTextFile("D:\\Spark\\result.txt")
  }

}
