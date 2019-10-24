package SparkSQL

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._


object displayTopSecondRecord {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()

    val spark = SparkSession.builder().master("local").appName("displayTopSecondRecord").config(conf = conf).getOrCreate()
    val sc = spark.sparkContext
    sc.setLogLevel("ERROR")

    /*
    Given the following schema,

    ORC file format:
    Product (product_id, product_name, product_type, product_version, product_price)
    Customer (customer_id, customer_first_name, customer_last_name, phone_number )
    Sales (transaction_id, customer_id, product_id, timestamp, total_amount, total_quantity )
    Refund (refund_id, original_transaction_id, customer_id, product_id, timestamp, refund_amount, refund_quantity)

    a) Display the customer name who made the second most purchases in the month of May 2013. Refunds should be excluded.
    b) Calculate the total amount of all transactions that happened in year 2013 and have not been refunded as of today.
    c) Find a product that has not been sold at least once (if any).
    d) Find customer with maximum revenue per day in the month of May 2013.

    ----------------------------
    */

    //val products = spark.read.option("inferschema", true).option("header", true).option("delimiter", ",").format("csv").load("G:\\Spark\\data-master\\SampleData\\products.csv")
    // products.show()
    val customers = spark.read.option("header", true).option("inferschema", true).option("delimiter", ",").option("encoding", "ISO-8859-1").csv("G:\\Spark\\data-master\\SampleData\\customers.csv")
    customers.show(1000)
    customers.printSchema()
    import spark.implicits._

    //val Customers = spark.read.option("inferschema", true).option("headers", true).load("G:\\Spark\\data-master\\SampleData\\customers.txt")

    val orders = spark.read.option("inferschema", true).option("header", true).format("csv").load("G:\\Spark\\data-master\\SampleData\\orders.csv")
    val orderdetail = spark.read.option("inferschema", true).option("header", true).format("csv").load("G:\\Spark\\data-master\\SampleData\\orderdetails.csv")
    val products = spark.read.option("inferschema", true).option("header", true).format("csv").option("encoding", "ISO-8859-1").load("G:\\Spark\\data-master\\SampleData\\products.csv")

    val customerJoinOrders = customers.join(orders, "CustomerID")
    //customerJoinOrders.show(5)
    val res = customerJoinOrders.select($"*").where("OrderDate LIKE '%1997'").groupBy("CustomerID", "CustomerName").agg(count("OrderID").alias("totalOrders")).orderBy(col("totalOrders").desc)
    //res.show()

    import org.apache.spark.sql.expressions._
    val rnk = Window.orderBy(col("totalOrders").desc)

    val res2 = dense_rank().over(rnk)
    res.select($"CustomerID", $"CustomerName", $"totalOrders", res2.alias("customerRank")).where("customerRank==2").show()
    val ordertotals = customerJoinOrders.join(orderdetail, "OrderID")
    val orderproducts = ordertotals.join(products, "ProductID").orderBy($"orderID".desc)
    orderproducts.show()
    val totalAmountPerOrder = orderproducts.select($"CustomerID", $"OrderID", (col("Quantity") * col("Price")).alias("totalorderAmount")).groupBy("CustomerID", "OrderID").agg(sum("totalorderAmount").alias("totalAmountperorder")).orderBy(col("totalAmountperorder").desc)
    totalAmountPerOrder.show()
    //totalAmountPerOrder.select($"CustomerID", $"OrderID", $"totalorderAmount").groupBy("CustomerID","orderID").agg(sum($"totalorderAmount")).orderBy($"totalorderAmount".desc).show()
    //totalAmountPerOrder.show()
    //totalAmountPerOrder.select($"*").groupBy("orderID").agg(sum("totalorderAmount")).orderBy($"totalorderAmount".desc)
    //val orderProducts = orderdetail.join(products,"ProductID")
    //customerJoinOrders.select("CustomerID","CustomerName","Country","OrderID","OrderDate").groupBy("CustomerID").agg(count("OrderID").alias("totalOrders")).orderBy(desc("totalOrders")).show()
    //val Refund = spark.read.option("inferschema", true).option("headers", true).load("G:\\Spark\\data-master\\SampleData\\products.txt")
    //products.show()
    // val CustomerJoinSale = customers.join(orders, "CustomerID")
    //CustomerJoinSale.show()
    // import spark.implicits._
    //val customerJoinSales = Customers.join(orders , "customerId")
    //CustomerJoinSale.select("*").agg(count("transaction_id").alias("totalorders")).where("timestamp LIKE '2013-05%' AND customerId NOT IN ('select customerId from Refunds')").orderBy("totalPurchase").show()

    //spark.read.textFile("G:\\Spark\\data-master\\SampleData\\products.txt")
  }
}
