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
import org.apache.spark.sql.types.LongType


object StaticStreamingJoin extends App{
  Logger.getLogger("org").setLevel(Level.ERROR)
  
  val spark = SparkSession.builder().master("local[*]").appName("My application")
  .config("spark.streaming.stopGracefullyOnShutdown",true)
  .config("spark.sql.shuffle.partitions",3)
  .getOrCreate

  
//  1.A Creating a Schema for Inference
  val transactionSchema = StructType(List(
      StructField("card_id",LongType),
      StructField("amount",IntegerType),
      StructField("postcode",IntegerType),
      StructField("pos_id",LongType),
      StructField("transaction_dt",TimestampType)
      ))
//  1. read from file Source
  val transactionsDf = spark.readStream
  .format("socket")
  .option("host","localhost")
  .option("port","12345")
  .load()
  
  val cardDf = spark.read
                .format("csv")
                .option("header",true)
                .option("inferSchema", true)
                .option("path","C:/Users/chnis/Downloads/Big Data/week16/carddetails.csv")
                .load()
//  2. Process
  val valueDf = transactionsDf.select(from_json(col("value"),transactionSchema).alias("value"))
  val refinedDf = valueDf.select("value.*")
  val joinExpr = refinedDf.col("card_id") === cardDf.col("card_id")
  val joinType = "inner"
  
  val enrichedDf = refinedDf.join(cardDf,joinExpr,joinType).drop(cardDf.col("card_id"))
   
                
//  refinedDf.printSchema()
//  3. write to sink.
val ordersQuery = enrichedDf.writeStream
.format("console")
.outputMode("update")
.option("checkpointLocation","checkpoint-location1")
.trigger(Trigger.ProcessingTime("15 second"))
.start()

 ordersQuery.awaitTermination()
}