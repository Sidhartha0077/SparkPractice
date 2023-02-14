package week11DataFrames

import org.apache.log4j.Level
import org.apache.log4j.Logger
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.Row
import org.apache.spark.sql.Dataset
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.types.StructField
import org.apache.spark.sql.types.IntegerType
import org.apache.spark.sql.types.TimestampType
import org.apache.spark.sql.types.StringType



//case class Order(order_id: Int, order_date: Timestamp, order_customer_id: Int, order_status: String )

object practiceAnyThing extends App{
  Logger.getLogger("org").setLevel(Level.ERROR)
  
  val sparkConf = new SparkConf()
  sparkConf.set("spark.app.name", "practice")
  sparkConf.set("spark.master", "local[2]")
  
  val spark = SparkSession.builder()
  .config(sparkConf)
  .getOrCreate()
  
  /*val schemaType = StructType(List(StructField("order_id", IntegerType, false),
      (StructField("order_date", TimestampType)),
      (StructField("order_customer_id", IntegerType)),
      (StructField("order_status", StringType))
      ))*/ 
  val schemaType = "order_id Int, order_date Timestamp, order_customer_id Int,order_status String"
  
  val ordersDF = spark.read
  .format("csv")
  .option("header", "true")
  .schema(schemaType)
  .option("mode", "DROPMALFORMED")
  .option("path","/Users/prince/Desktop/week11/orders-201019-002101.csv")
  .load()
  
  //import spark.implicits._
  
 ordersDF.printSchema()
  
   ordersDF.createOrReplaceTempView("orderTable")
  
  
  //spark.sql("""select *, case when order_status = 'COMPLETE' then concat('status_', order_status) 
    //else order_status end as conc from orderTable""").show
  
  spark.sql("""update table orderTable where order_status= 'COMPLETE'
    
   """).show()
  
  
  
  
  
  spark.stop()
  
}