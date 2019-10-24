package SparkSQL

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession


object removeSpaces {
  def main(args: Array[String]): Unit = {


    val conf = new SparkConf()
    val spark = SparkSession.builder().master("local").config(conf = conf).appName("removeSpaces").getOrCreate()
    val sc = spark.sparkContext
    sc.setLogLevel("ERROR")
    import spark.implicits._

    val df = Seq(("  p a   b l o", "Paraguay"),
      ("Neymar", "B r    asil")
    ).toDF("name", "country")

    df.show()

  }


}
