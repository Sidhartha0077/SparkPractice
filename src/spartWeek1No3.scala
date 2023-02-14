import org.apache.spark.SparkContext
import org.apache.log4j.Level
import org.apache.log4j.Logger

object scalaPractice3 extends App {
  
Logger.getLogger("org").setLevel(Level.ERROR)
  
val sc = new SparkContext("local[*]","wordcount")
  
val input = sc.textFile("/Users/prince/Desktop/moviedata-201008-180523.data")
  
val relColumn = input.map(x=> (x.split("\t")(2)))

// val result = relColumn.countByValue

// but countByValues doesn't produce rdd it's an action so it will run locally and there will be no parallelism for further activities.

// result.foreach(println)

val indRating = relColumn.map(x=> (x,1))

val redByRating = indRating.reduceByKey((x,y)=> x+y)

val sortedRating = redByRating.sortBy(x=> x._2)


sortedRating.collect.foreach(println)


}