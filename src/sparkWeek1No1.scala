import org.apache.spark.SparkContext
import org.apache.log4j.Level
import org.apache.log4j.Logger



object scalaPractice1 extends App{
 
Logger.getLogger("org").setLevel(Level.ERROR)

val sc = new SparkContext("local[*]","wordcount")
  
val input = sc.textFile("/Users/prince/Desktop/friends.txt")
  
val word = input.flatMap(x=> x.split("::"))
  
val lower = word.map(_.toLowerCase())

val wordTuple = lower.map(x=> (x,1))


val finalcount = wordTuple.reduceByKey((a,b) => a+b)

val fcex = finalcount.map(x=> (x._2,x._1))
  
val finalAsc = fcex.sortByKey(false)



wordTuple.collect.foreach(println)
 
//scala.io.StdIn.readLine()

}