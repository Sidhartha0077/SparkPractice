package week11DataFrames

import org.apache.log4j.Level
import org.apache.log4j.Logger
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.IntegerType
object practiceRandom extends App{
  Logger.getLogger("org").setLevel(Level.ERROR)
  
  val sparkConf = new SparkConf()
  sparkConf.set("spark.master", "local[2]")
  
  val spark = SparkSession.builder()
  .config(sparkConf)
  .getOrCreate()
  
val myList = List((1,"2013-07-25",11599,"CLOSED"),(2,"2014-07-25",256,"PENDING_PAYMENT"),
(3,"2013-07-25",11599,"COMPLETE"),(4,"2019-07-25",8827,"CLOSED"))
  
val df1 = spark.createDataFrame(myList).toDF("id", "date", "pdtid", "status") 
  
//val df2 = df1.toDF("id", "date", "pdtid", "status")

df1.show

val df3 = df1.withColumn("pdtid", col("pdtid").cast(IntegerType))
.agg(sum("pdtid").as("sumpdtid"), sum(expr("pdtid * 100")).as("multiplier"))
.show()




spark.stop()


}