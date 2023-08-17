

import org.apache.log4j.Level
import org.apache.log4j.Logger
import org.apache.spark.SparkContext


object MovieRating extends App{
  Logger.getLogger("org").setLevel(Level.ERROR)
  val sc=new SparkContext("local[*]","customerdata")
  val input=sc.textFile("C:/Users/chnis/Downloads/Big Data/week9 -Spark1/moviedata.data")
  val mappedInput=input.map(x=>(x.split("\t")(2).toInt))
  val res=mappedInput.countByValue()
//  val ratings=mappedInput.map(x=>(x,1))
//  val reducedRatings=ratings.reduceByKey((x,y)=>(x+y))
//  val res=reducedRatings.collect
  
  res.foreach(println)
}