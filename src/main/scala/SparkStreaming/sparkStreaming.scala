//spark-shell --master local[2]
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.streaming._
object sparkStreaming {

  def main(args: Array[String]): Unit = {


    val conf = new SparkConf()
    val spark = SparkSession.builder().master("local").config(conf = conf).appName("sparkStreaming").getOrCreate()
    val sc = spark.sparkContext


    val ssc = new StreamingContext(sc, Seconds(10)) //10 seconds is microbatch period
    val data = ssc.socketTextStream("localhost", 9999)
    //val data = ssc.socketTextStream("localhost", 9999)
    val word = data.flatMap(x => x.split("\\W+"))
    val pairs = word.map(x => (x, 1))
    val res = pairs.reduceByKey(_ + _)
    res.print()
    ssc.start //streaming started
    import spark.implicits._

    //Ex 2)Sliding and Windowing
    pairs.persist()
    pairs.reduceByKeyAndWindow((a: Int, b: Int) => (a + b), Seconds(30), Seconds(10), 2)
    //pairs.reduceByKeyAndWindow(_+_,20,40)//20 seconds for sliding and 40 seconds for windowing
    //sliding period is greater than equal to micro batch period.

  }
}