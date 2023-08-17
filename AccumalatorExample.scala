import org.apache.spark.SparkContext
import org.apache.log4j.Level
import org.apache.log4j.Logger


object AccumalatorExample extends App{
  Logger.getLogger("org").setLevel(Level.ERROR)
  val sc = new SparkContext("local[*]", "accumalatorexample")
  val rdd1=sc.textFile("C:/Users/chnis/Downloads/Big Data/week 10/samplefile.txt")
  val myaccu=sc.longAccumulator("count blank lines")
  rdd1.foreach(x=> if (x=="") myaccu.add(1))
  println(myaccu.value)
}