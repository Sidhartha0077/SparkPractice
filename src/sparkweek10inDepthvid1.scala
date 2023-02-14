import org.apache.spark.SparkContext

import org.apache.log4j.Logger
import org.apache.log4j.Level

object sparkweek10inDepthvid1 extends App{
Logger.getLogger("org").setLevel(Level.ERROR)
val sc = new SparkContext("local[*]","wordcount")
  
val myList = List("warn: tuesday1", "warn: tuesday2", "warn: tuesday3",
    
                  "warn: tuesday3","error: tuesday2","error: tuesday2")
// myList is in local we need to create rdd by using parallelize key word
                  
 val parallelList = sc.parallelize(myList)              
                  
 val warning = parallelList.map(x=> (x.split(":")(0),1))
 
 val reduced = warning.reduceByKey((x,y) => x+y)                 
                  
 reduced.collect.foreach(println)             
                                
                  
}