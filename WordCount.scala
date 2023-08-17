import org.apache.spark.SparkContext
import org.apache.log4j.Level
import org.apache.log4j.Logger


object WordCount extends App{
  Logger.getLogger("org").setLevel(Level.ERROR)
  val sc=new SparkContext()
  val input=sc.textFile("/data/search_data.txt")
  val words=input.flatMap(_.split(" "))
  val wordsLower=words.map(_.toLowerCase())
  val wordCount=wordsLower.map((_,1))
  val res=wordCount.reduceByKey((_+_))
//  val revRes=res.map(x=>(x._2,x._1))
  val top10=res.sortBy(x=>x._2, false)
  val results=top10.collect
  for(result <- results)
  {
    val word=result._1
    val count=result._2
    println(s"$word : $count")
  }
//  scala.io.StdIn.readLine()
}