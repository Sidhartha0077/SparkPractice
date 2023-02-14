package week11DataFrames
import org.apache.log4j.Level
import org.apache.log4j.Logger
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.Row
import org.apache.spark.sql.Dataset
import java.security.Timestamp

case class OrderData(order_id: Int, orders_date: Timestamp, order_customer_id: Int, Order_status: String)

object practiceDS {
 Logger.getLogger("org").setLevel(Level.ERROR)
 
 val sparkConf = new SparkConf()
 sparkConf.set("spark.app.name", "my app")
 sparkConf.set("spark.master", "local[2]")
 
 val spark = SparkSession.builder()
 .config(sparkConf)
 .getOrCreate()
 
val ordersDF: Dataset[Row] = spark.read
.format("csv")
.option("header", "true")
.option("inferschema", "true")
.option("mode", "DROPMALFORMED")
.option("path", "/Users/prince/Desktop/week11/orders-201019-002101.csv")
.load()

import spark.implicits._
val ordersDs = ordersDF.as[OrderData].show()
 

 
 
 
 
}