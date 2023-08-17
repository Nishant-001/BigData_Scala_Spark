import org.apache.log4j.Level
import org.apache.log4j.Logger
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.streaming.Trigger
import org.apache.spark.sql.types.IntegerType
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.types.StructField
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.StringType
import org.apache.spark.sql.types.TimestampType


object StreamingTumblingAndSlidingWindow extends App{
  Logger.getLogger("org").setLevel(Level.ERROR)
  
  val spark = SparkSession.builder().master("local[*]").appName("My application")
  .config("spark.streaming.stopGracefullyOnShutdown",true)
  .config("spark.sql.shuffle.partitions",3)
  .getOrCreate

  
//  1.A Creating a Schema for Inference
  val orderSchema = StructType(List(
      StructField("order_id",IntegerType),
      StructField("order_date",TimestampType),
      StructField("order_customer_id",IntegerType),
      StructField("order_status",StringType),
      StructField("amount",IntegerType)
      ))
//  1. read from file Source
  val ordersDf = spark.readStream
  .format("socket")
  .option("host","localhost")
  .option("port","1234")
  .load()
//  2. Process
  val valueDf = ordersDf.select(from_json(col("value"), orderSchema).alias("value"))
  val refinedDf = valueDf.select("value.*")
  val windowAggDf = refinedDf
                    .withWatermark("order_date", "30 minute") 
                    .groupBy(window(col("order_date"),"15 minute"))
                    .agg(sum("amount").alias("totalInvoice"))
  val outputDf = windowAggDf.select("window.start","window.end","totalInvoice")
  
//  outputDf.printSchema()
  
//  3. write to sink.
val ordersQuery = outputDf.writeStream
.format("console")
.outputMode("update")
.option("checkpointLocation","checkpoint-location1")
.trigger(Trigger.ProcessingTime("15 second"))
.start()

 ordersQuery.awaitTermination()
}