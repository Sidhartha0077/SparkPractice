package week11DataFrames

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.IntegerType
import org.apache.spark.sql.types.StringType
import org.apache.spark.sql.types.TimestampType
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.types.StructField
import org.apache.spark.sql.SaveMode
import org.apache.log4j.Level
import org.apache.log4j.Logger

case class windowData(Country: String, Weeknum: String, Numinvoice: String, TotalQuantity: String, InvoiceValue: String)

object practiceSchema extends App{
  Logger.getLogger("org").setLevel(Level.ERROR)
  val sparkConf = new SparkConf()
  sparkConf.set("spark.app.name", "my app")
  sparkConf.set("spark.master", "local[*]")
  
  val spark = SparkSession.builder()
  .config(sparkConf)
  .getOrCreate()

/*val orderSchema = StructType(List(
StructField("orderid", IntegerType, true),   
StructField("orderdate", TimestampType, false),
StructField("customerid", IntegerType),
StructField("status", StringType)
))*/

val schemaDDL = "country String, weeknum Int, numinvoice Int, totalquantity Int, invoicevalue Double"
 
val ordersDF = spark.read
.format("csv")
.option("header", "true")
.schema(schemaDDL)
.option("path", "/Users/prince/Desktop/week12/windowdata.csv")
.load()


/*ordersDF.write
.format("parquet")
.partitionBy("country", "weeknum")
.mode(SaveMode.Overwrite)
.option("path", "C:/Users/prince/Desktop/week12/practice_output")
.save()*/
  
  
  spark.sql("create database if not exists retail1")
  
  ordersDF.write
  .format("parquet")
  .partitionBy("country","weeknum")
  .mode(SaveMode.Overwrite)
  .saveAsTable("retail1.tab1")
  
  
  
  
  
 //import spark.implicits._ 
 
 //val widowRdd = spark.sparkContext.textFile("/Users/prince/Desktop/week12/windowdata.csv")  
  
 //val windowDf = widowRdd.map(x=> x.split(",")).map(x=> windowData(x(0), x(1), x(2), x(3), x(4))).toDF().repartition(4)
 
 //windowDf.show
 
 
 
 
 
  
  spark.stop()
  
  
   
  
  
  
  
}