import org.apache.spark.SparkContext
import org.apache.log4j.Level
import org.apache.log4j.Logger
// to find out total amnt. spent per key word searches

object week10vid1 extends App {
  Logger.getLogger("org").setLevel(Level.ERROR)
  val sc = new SparkContext("local[*]","wordcount")
  
  val input = sc.textFile("/Users/prince/Desktop/New folder/bigdatacampaigndata-201014-183159.csv")
  
  val twoLines = input.map(x=> (x.split(",")(10),x.split(",")(0))) 
  
  val wordSplit = twoLines.flatMapValues(x=> x.split(" "))
  
  val correctOrder = wordSplit.map(x=> (x._2.toLowerCase, x._1.toFloat))
    
  val finalReduced =  correctOrder.reduceByKey((x,y) => x+y).sortBy(x=> x._2,false)
 
  //val singleValue = finalReduced.map(x=> x._2).sum().toInt
  
  wordSplit.collect.foreach(println)
  //println(singleValue)
  
}