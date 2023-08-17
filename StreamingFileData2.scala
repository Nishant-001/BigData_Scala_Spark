import org.apache.log4j.Level
import org.apache.log4j.Logger
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.streaming.Trigger


object StreamingFileData2 extends App{
  Logger.getLogger("org").setLevel(Level.ERROR)
  
  val spark = SparkSession.builder().master("local[*]").appName("My application")
  .config("spark.sql.shuffle.partitions",3)
  .config("spark.sql.streaming.schemaInference","true")
  .getOrCreate
//  1. read from file Source
  val ordersDf = spark.readStream
  .format("json")
  .option("path","C:/Users/chnis/Downloads/Big Data/week16/orders")
  .option("maxFilesPerTrigger",1)
  .load()
  
//  2. Process
  ordersDf.createOrReplaceTempView("orders")
  val completedOrders = spark.sql("select count(*) from orders where order_status = 'COMPLETE'")
  
  
//  3. write to sink.
  val ordersQuery = completedOrders.writeStream
  .format("console")
  .outputMode("complete") // append/complete/update
//  .option("path","C:/Users/chnis/Downloads/Big Data/week16/outputfolder")
  .option("checkpointLocation","checkpoint-location1")
  .trigger(Trigger.ProcessingTime("10 seconds"))
  .start()
  
  ordersQuery.awaitTermination()
}