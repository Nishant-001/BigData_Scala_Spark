import org.apache.log4j.Level
import org.apache.log4j.Logger
import org.apache.spark.SparkContext


object CustomerData extends App{
  Logger.getLogger("org").setLevel(Level.ERROR)
  val sc=new SparkContext("local[*]","customerdata")
  val input=sc.textFile("C:/Users/chnis/Downloads/Big Data/week 9/customerorders.csv")
  val mappedInput=input.map(x=>(x.split(",")(0),x.split(",")(2).toFloat))
  val totalByCustomer=mappedInput.reduceByKey((x,y)=>(x+y))
  val sortedTotal=totalByCustomer.sortBy(x=>x._2)
  
  sortedTotal.saveAsTextFile("C:/Users/chnis/Desktop/Spark result/customerData")
//  val result=sortedTotal.collect
//  result.foreach(println)
}