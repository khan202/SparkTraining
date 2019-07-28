package SparkSQL

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

/**
  * Created by ImranKhan on 27-07-2019.
  */
object analyticalAndWindowingFunctions {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
    val spark = SparkSession.builder().master("local").config(conf = conf).appName("DSLoperation2").getOrCreate()
    val sc = spark.sparkContext
    sc.setLogLevel("ERROR")
    val employeeData = sc.textFile("D:\\Spark\\data-master\\data-master\\hr_db\\employees\\part-00000.csv")
    // employeeData.take(10).foreach(println)
    /* // 100	Steven	King	SKING	515.123.4567	1987-06-17	AD_PRES	24000.00	null	null	90
      // 100	Steven	King	SKING	515.123.4567	1987-06-17	AD_PRES	24000.00	null	null	90
      101	Neena	Kochhar	NKOCHHAR	515.123.4568	1989-09-21	AD_VP	17000.00	null	100	90
      102	Lex	De Haan	LDEHAAN	515.123.4569	1993-01-13	AD_VP	17000.00	null	100	90
      103	Alexander	Hunold	AHUNOLD	590.423.4567	1990-01-03	IT_PROG	9000.00	null	102	60*/

    val employeeRDD = employeeData.map(e => {
      val w = e.split("\t")
      (w(0).toInt, w(1), w(2), w(3), w(4), w(5), w(6), w(7).toFloat,
        if (w(8) != "null") w(8).toFloat else 0.0f,
        if (w(9) != "null") w(9).toInt else 0,
        if (w(10) != "null") w(10).toInt else 0)
    })
    import spark.implicits._
    val employee = employeeRDD.toDF("employee_id", "fname", "lname", "email", "phone", "hire_date", "role", "salary", "commission_pct",
      "manager_id", "department_id")

    import org.apache.spark.sql.expressions._
    employee.select($"*", sum("salary").over(Window.partitionBy("department_id")).alias("department_expence")).show()
    //employee.select($"department_id", sum("salary").over(Window.partitionBy("department_id")).alias("department_expence")).groupBy("department_id").count().show()
    val spec = Window.partitionBy("department_id")
    // employee.select($"employee_id", $"department_id", $"salary", sum("salary").over(spec).alias("department_expence")).show()
    print("Average salary")
    //employee.select($"employee_id", $"department_id", $"salary", avg("salary").over(spec).alias("department_expence")).show()
    print("Max salary")
    println()
    // employee.select($"employee_id", $"department_id", $"salary", max("salary").over(spec).alias("department_expence")).show()
    print("Min salary")
    println()
    //employee.select($"employee_id", $"department_id", $"salary", min("salary").over(spec).alias("department_expence")).show()
    val spec1 = Window.partitionBy("department_id").orderBy($"salary".desc)
    val rnk = rank().over(spec1)
    val empRank = employee.select($"employee_id", $"department_id", $"salary", rnk.alias("EmpRank")) //.show()

    val spec2 = Window.partitionBy("department_id").orderBy(col("salary").desc)
    val denseRnk = dense_rank().over(spec1)
    val empRanked = employee.select($"employee_id", $"department_id", $"salary", denseRnk.alias("EmpDenseRank")) //.show()
    val rn = row_number().over(spec1).alias("rn")
    //val emp_rn = employee.select($"employee_id", $"department_id", $"salary", rn).show()
    //val sal_rnk = rank().over(spec1)

    empRank.where("empRank<=3").show()
    empRank.select("employee_id", "empRank").where("empRank<=3").show()

    val spec3 = Window.partitionBy("department_id").orderBy($"salary".desc)
    val lead1 = lead("salary", 1).over(spec3).alias("Next_salary")
    //lead ( column name, offset = to the next value => exact next value or skip any values in between)
    val empLead = employee.select($"employee_id", $"department_id", $"salary", lead1).show()

    val lag1 = lag("salary", 1).over(spec3).alias("previos_salary")
    val empLag = employee.select($"employee_id", $"department_id", $"salary", lag1).show()


  }

}
