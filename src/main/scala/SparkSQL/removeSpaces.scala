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
//*******************Remove spaces in between the rows data***********************************//
    val df = Seq(("  p a   b l o", "Paraguay"),
      ("Neymar", "B r    asil")
    ).toDF("name", "country")
    import org.apache.spark.sql.functions._
    df.show()
    val actualdf = Seq("name", "country").foldLeft(df) { (memoDF, colName) =>
      memoDF.withColumn(colName, regexp_replace(col(colName),"\\s+",""))

    }
    actualdf.show()
//***********************remove spaces in the between the column names and change to lowercase*********************//
    val sourceDF = Seq(
      ("funny", "joke")
    ).toDF("A b C", "de F")

    sourceDF.show()
    val actualDF = sourceDF
      .columns
      .foldLeft(sourceDF) { (memoDF, colName) =>
        memoDF
          .withColumnRenamed(
            colName,
            colName.toLowerCase().replace(" ", "_")
          )
      }

    actualDF.show()



  }


}
