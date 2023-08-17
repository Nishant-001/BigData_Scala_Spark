import org.apache.spark.SparkContext
import org.apache.log4j.Level
import org.apache.log4j.Logger


object LogLevelGrouping extends App{
  Logger.getLogger("org").setLevel(Level.ERROR)
  val sc = new SparkContext("local[*]", "wordcount")
  val rdd1=sc.textFile("C:/Users/chnis/Downloads/Big Data/week 10/bigLog.txt")
  
  val mappedrdd= rdd1.map(x => {
    val fields=x.split(":")
    (fields(0),1)
  })
  mappedrdd.reduceByKey(_+_).collect.foreach(println)
  scala.io.StdIn.readLine()
  }