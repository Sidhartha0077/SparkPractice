import org.apache.spark.SparkContext
import org.apache.log4j.Level
import org.apache.log4j.Logger

object spartWeek1No4 extends App{
  
Logger.getLogger("org").setLevel(Level.ERROR)

val sc = new SparkContext("local[*]","wordcount")

val input = sc.textFile("/Users/prince/Desktop/friends.csv")

val splitted = input.map(x=> (x.split("::")(2).toInt,x.split("::")(3).toInt))

//val tup = splitted.map(x=> (x._1,(x._2,1)))

// alternative method
val tup = splitted.mapValues(x=> (x,1))

val reduced = tup.reduceByKey((x,y)=> (x._1+y._1,x._2+y._2))

// val avg = reduced.map(x=> (x._1,x._2._1/x._2._2)).sortBy(x=> x._2)

// alternative method
val avg = reduced.mapValues(x=> x._1/x._2).sortBy(x=> x._2)


val a = avg.collect

a.foreach(println)









}