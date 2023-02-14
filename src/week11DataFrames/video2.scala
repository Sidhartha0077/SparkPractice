package week11DataFrames
import org.apache.log4j.Level
import org.apache.log4j.Logger
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.functions.split
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.Row
object video2 extends App{
Logger.getLogger("org").setLevel(Level.ERROR)
//To create spark session
val spark = SparkSession.builder().appName("My application 1").master("local[2]").getOrCreate()

val sc = spark.sparkContext

val input = sc.textFile("/Users/prince/Desktop/friends - Copy1.csv")

/*val input = spark.read
.option("header",true)
.option("inferSchema",true)
.csv("/Users/prince/Desktop/week11/orders-201019-002101.csv")*/

val rdd1 = input.map(x=> (x.split(" ")(0),x.split(" ")(1)))


val df = spark.createDataFrame(rdd1).toDF("c1","c2")
df.withColumn("c1Split",split(col("c1"),","))
.withColumn("c3",col("c1Split")(0))
.select(substring(col("c3"),-1,1).as("final"))

.show


















// To throw a message at the end of a task
Logger.getLogger(getClass.getName).info("my application is completed successfully")

//input.show()
//input.printSchema()




spark.stop()
}