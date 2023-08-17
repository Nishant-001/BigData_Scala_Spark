import org.apache.spark.SparkContext
import org.apache.log4j.Level
import org.apache.log4j.Logger


object Loglevel extends App {
Logger.getLogger("org").setLevel(Level.ERROR)
val sc = new SparkContext("local[*]", "wordcount")
val mylist = List("WARN: Tuesday 4 September 405",
"ERROR: Tuesday 4 September 408",
"ERROR: Tuesday 4 September 408",
"ERROR: Tuesday 4 September 0408",
"ERROR: Tuesday 4 September 408",
"ERROR: Tuesday 4 September 0408")
sc.parallelize(mylist).map(x => (x.split(":")(0),1)).reduceByKey(_+_). collect().foreach(println)
}