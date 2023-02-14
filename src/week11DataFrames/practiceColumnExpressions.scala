package week11DataFrames
import org.apache.log4j.Level
import org.apache.log4j.Logger
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._


object practiceColumnExpressions extends App{
 Logger.getLogger("org").setLevel(Level.ERROR)
 
def ageCheck(age: Int)= {
   if (age > 18) "y"
   else "N"
 }
 val sparkConf = new SparkConf()
  sparkConf.set("spark.app.name", "my app")
  sparkConf.set("spark.master", "local[*]")
  
  val spark = SparkSession.builder()
  .config(sparkConf)
  .getOrCreate()

val parseAge = udf(ageCheck(_:Int):String)  
  
val ordersDF = spark.read
.format("csv")
.option("header", "true")
.option("inferschema", "true")
.option("path", "/Users/prince/Desktop/week11/orders-201019-002101.csv")
.load() 







scala.io.StdIn.readLine()
spark.stop()
}