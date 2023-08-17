import org.apache.log4j.Level
import org.apache.log4j.Logger
import org.apache.spark.SparkContext
import scala.io.Source


object Bigdata extends App{
  
  def loadBoaringWords():Set[String] ={
    var boringWords:Set[String]=Set()
    val lines = Source.fromFile("C:/Users/chnis/Downloads/Big Data/week 10/boringwords.txt").getLines()
    for(line <- lines){
      boringWords+=line
    }
    boringWords
  }
  Logger.getLogger("org").setLevel(Level.ERROR)
  val sc = new SparkContext("local[*]", "bigdatacamp")
  var nameSet = sc.broadcast(loadBoaringWords)
  val input =sc.textFile("C:/Users/chnis/Downloads/Big Data/week 10/bigdatacampaigndata.csv")
  val mappedInput = input.map(x => (x.split(",")(10).toFloat,x.split(",")(0)))
  val words = mappedInput.flatMapValues(x =>x.split(" "))
  val finalwords= words.map(x =>(x._2.toLowerCase(),x._1))
  val filterres=finalwords.filter(x => !nameSet.value(x._1))
  val total=filterres.reduceByKey((x,y)=>x+y).sortBy(x=>x._2,false)
  
  val res=total.collect
  res.take(10).foreach(println)
  
}