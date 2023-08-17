import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.log4j.Level
import org.apache.log4j.Logger
import org.apache.spark.sql.SaveMode


object writeDemo extends App{
  Logger.getLogger("org").setLevel(Level.ERROR)
  val sparkConf=new SparkConf()
  sparkConf.set("spark.app.name","My Application 1")
  sparkConf.set("spark.master","local[2]")
  val spark=SparkSession.builder()
  .config(sparkConf)
  .enableHiveSupport()
  .getOrCreate()
  
  val ordersDf = spark.read
  .format("csv")
  .option("header",true)
  .option("inferSchema",true)
  .option("path", "C:/Users/chnis/Downloads/Big Data/week12/orders.csv")
  .load()
  
  spark.sql("create database if not exists retail")
  
   ordersDf.write
  .format("csv")
  .mode(SaveMode.Overwrite)
  .bucketBy(4, "order_customer_id")
  .sortBy("order_customer_id")
  .saveAsTable("retail.orders")
  

  
//  ordersDf.write
//  .format("csv")
//  .mode(SaveMode.Overwrite)
//  .option("path", "C:/Users/chnis/Downloads/Big Data/week11/windowData_jsonoutput")
//  .save()
}