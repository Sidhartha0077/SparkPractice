package week11DataFrames
import org.apache.log4j.Level
import org.apache.log4j.Logger
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession



object practice extends App{
Logger.getLogger("org").setLevel(Level.ERROR)

val sparkCof = new SparkConf()
sparkCof.set("spark.application", "my application")
sparkCof.set("spark.master", "local[2]")
sparkCof.set("spark.driver.memory","500M")

val spark = SparkSession.builder()
.config(sparkCof)
.getOrCreate()

val ordersDF = spark.read
.format("csv")
.option("header", "true")
.option("inferschema", "true")
.option("path", "/Users/prince/Desktop/week11/orders-201019-002101.csv")
.load()

val filterDF = ordersDF.repartition(4)
.select("order_id","order_date", "order_status")
.groupBy("order_status")
.count()
.where("count > 10000")

val lower = ordersDF.rdd.repartition(2)


filterDF.show

Logger.getLogger("getClass.getName").info("my app completed successfully")
scala.io.StdIn.readLine()
spark.stop()

}