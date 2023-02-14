import org.apache.spark.SparkContext
import org.apache.log4j.Level
import org.apache.log4j.Logger
import scala.io.Source
// Filter boring words from the search key word data

object week10vid2 extends App {
  
  def loadBoringWords(): Set[String] = {
    var boringWords:Set[String] = Set()
    val lines = Source.fromFile("/Users/prince/Desktop/New folder/boringWords.txt").getLines()
    
    for (i<- lines){
      boringWords = boringWords + i  
    }
    boringWords
  }
   Logger.getLogger("org").setLevel(Level.ERROR)
  
  val sc = new SparkContext("local[*]","wordcount")
   
  var boringBroadcast = sc.broadcast(loadBoringWords)
  
  val input = sc.textFile("/Users/prince/Desktop/New folder/bigdatacampaigndata-201014-183159.csv")

  val twoLines = input.map(x=> (x.split(",")(10),x.split(",")(0))) 
  
  val wordSplit = twoLines.flatMapValues(x=> x.split(" "))
  
  val correctOrder = wordSplit.map(x=> (x._2.toLowerCase, x._1.toFloat))
  
  val boringWordFiltered = correctOrder.filter(x=> !boringBroadcast.value(x._1))
  
  val finalReduced =  boringWordFiltered.reduceByKey((x,y) => x+y).sortBy(x=> x._2,false)
 
 
  
  finalReduced.collect.foreach(println)
  











}