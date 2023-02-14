package week11DataFrames
import org.apache.log4j.Level
import org.apache.log4j.Logger
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.Dataset

object video1 extends App{
Logger.getLogger("org").setLevel(Level.ERROR)
//To create spark session
val spark = SparkSession.builder().appName("My application 1").master("local[2]").getOrCreate()

//val sc = spark.sparkContext

//sc.textFile("/Users/prince/Desktop/week11/orders-201019-002101.csv", 8)

val input = spark.read
.option("header",true)
.option("inferSchema",true)
.csv("/Users/prince/Desktop/week11/orders-201019-002101.csv")


val groupedOrders = input
.repartition(4)
.where("order_customer_id > 10000")
.select("order_id","order_customer_id")
.groupBy("order_customer_id")
.count()
groupedOrders.show()




// To throw a message at the end of a task
Logger.getLogger(getClass.getName).info("my application is completed successfully")

//input.show()
//input.printSchema()




spark.stop()
}