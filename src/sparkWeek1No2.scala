import org.apache.spark.SparkContext
import org.apache.log4j.Level
import org.apache.log4j.Logger

object scalaPractice2 extends App{
  
Logger.getLogger("org").setLevel(Level.ERROR)

val sc = new SparkContext("local[*]","wordcount")
  
val input = sc.textFile("/Users/prince/Desktop/customerorders-201008-180523.csv")
  
val splitvalues = input.map(x=> (x.split(",")(0),x.split(",")(2).toFloat))

val spentvalue = splitvalues.reduceByKey((x,y)=> x+y)

val sortedTotal = spentvalue.sortBy(x=> x._2)

val b = for (sortedTotal<-sortedTotal){
  val b = sortedTotal._1
  val a = sortedTotal._2
  if (a> 4000)println(s"$b:$a")
}
 

    
scala.io.StdIn.readLine()

}