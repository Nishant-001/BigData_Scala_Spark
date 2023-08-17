import org.apache.spark.SparkContext
import org.apache.log4j.Level
import org.apache.log4j.Logger


object MovieRating2 extends App{
  Logger.getLogger("org").setLevel(Level.ERROR)
  val sc=new SparkContext("local[*]","wordcount")
  val input=sc.textFile("C:/Users/chnis/Downloads/Big Data/week11/ratings.dat")
  val movieRDD=sc.textFile("C:/Users/chnis/Downloads/Big Data/week11/movies.dat")
  val mappedMoviesRDD=movieRDD.map(x => {
    val fields=x.split("::")
    (fields(0),fields(1))
  })
  val mappedRDD=input.map(x => {
    val fields=x.split("::")
    (fields(1),fields(2))
  })
  val newMappped=mappedRDD.mapValues(x =>(x.toFloat,1.0))
  val reducedRdd=newMappped.reduceByKey((x,y) =>(x._1+y._1,x._2+y._2))
  val filteredRDD=reducedRdd.filter( x=> x._2._2>10)
  val topRating=filteredRDD.mapValues(x=>x._1/x._2).filter(x=> x._2>4.0)
  topRating.collect().foreach(println)
  
  val joinedRDD=mappedMoviesRDD.join(topRating)
  val finalres=joinedRDD.map(x => x._2._1)
  
  finalres.collect().foreach(println)
  scala.io.StdIn.readLine()
}