import org.apache.log4j.Level
import org.apache.log4j.Logger
import org.apache.spark.SparkContext
// Accumulator creation and execution

object week10vid3 extends App{
  Logger.getLogger("org").setLevel(Level.ERROR)
  
  val sc = new SparkContext("local[*]","wordcount")
     
  val input = sc.textFile("/Users/prince/Desktop/New folder/emptyLinesFile.txt")

  val myAccumulator = sc.longAccumulator("Blank lines accumulator")
  
  val x = input.foreach(x=> if (x=="") myAccumulator.add(1))

   println(myAccumulator.value)

}