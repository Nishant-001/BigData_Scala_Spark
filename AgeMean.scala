

import org.apache.log4j.Level
import org.apache.log4j.Logger
import org.apache.spark.SparkContext


object AgeMean extends App{
  def parser(line:String)={
    val age=line.split("::")(2).toInt
    val count=line.split("::")(3).toInt
    (age,count)
  }
  Logger.getLogger("org").setLevel(Level.ERROR)
  val sc=new SparkContext("local[*]","customerdata")
  val input=sc.textFile("C:/Users/chnis/Downloads/Big Data/week 9 Scala And Spark/friendsdata.csv")
  val mappedInput=input.map(parser)
//  val mapped1=mappedInput.map(x=>(x._1,(x._2,1)))
  val mapped1=mappedInput.mapValues(x=>(x,1))

  val mapped2=mapped1.reduceByKey((x,y)=>(x._1+y._1,x._2+y._2))
//  val mapped3=mapped2.map(x=>(x._1,x._2._1/x._2._2)).sortBy(x=>x._2)
  val mapped3=mapped2.mapValues(x=>(x._1/x._2)).sortBy(x=>x._2)

  val res=mapped3.collect
  res.foreach(println)
}