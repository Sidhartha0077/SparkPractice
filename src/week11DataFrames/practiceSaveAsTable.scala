package week11DataFrames

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.log4j.Level
import org.apache.log4j.Logger
import org.apache.spark.sql.SaveMode

object practiceSaveAsTable extends App {
  Logger.getLogger("org").setLevel(Level.ERROR)
  val sparkConf = new SparkConf()
  sparkConf.set("spark.app.name", "my app")
  sparkConf.set("spark.master", "local[*]")
  
  val spark = SparkSession.builder()
  .config(sparkConf)
  .enableHiveSupport()
  .getOrCreate()
  
val ordersDF = spark.read
.format("csv")
.option("header", "true")
.option("inferschema", "true")
.option("path", "/Users/prince/Desktop/week12/windowdata.csv")
.load() 
import spark.implicits._
spark.sql("create database if not exists retail")

spark.sql("create table if not exists  retail.hiveTable(id Int, status String) row format delimited fields terminated by ','")

spark.catalog .listTables("retail").show

ordersDF.write
.format("csv")
.mode(SaveMode.Overwrite)
.bucketBy(4, "numinvoices")
.sortBy("numinvoices")
.saveAsTable("retail.oreders")  
  
spark.catalog.listTables("retail").show()

scala.io.StdIn.readLine()
spark.stop()
}