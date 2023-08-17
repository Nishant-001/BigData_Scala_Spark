import org.apache.log4j.Level
import org.apache.log4j.Logger
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.streaming.Trigger


object StreamingWordCount extends App{
  Logger.getLogger("org").setLevel(Level.ERROR)
  
  val spark = SparkSession.builder().master("local[*]").appName("My application")
  .config("spark.sql.shuffle.partitions",3).getOrCreate
//  1. read from stream
  val linesDf = spark.readStream
  .format("socket")
  .option("host","localhost")
  .option("port","1234")
  .load()
  
//  2. Process
  
  val wordsDf = linesDf.selectExpr("explode(split(value,' ')) as word")
  val countsDf = wordsDf.groupBy("word").count()
  
//  3. write to sink.
  val wordCountQuery = countsDf.writeStream
  .format("console")
  .outputMode("update") // append/complete/update
  .option("checkpointLocation","checkpoint-location1")
  .trigger(Trigger.ProcessingTime("30 seconds"))
  .start()
  
  wordCountQuery.awaitTermination()
}