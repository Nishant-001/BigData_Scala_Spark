

import org.apache.log4j.Logger
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._
import org.apache.log4j.Level

object WindowAggregations extends App{
  Logger.getLogger("org").setLevel(Level.ERROR)
  val sparkConf=new SparkConf()
  sparkConf.set("spark.app.name","My Application 1")
  sparkConf.set("spark.master","local[2]")
  val spark=SparkSession.builder()
  .config(sparkConf)
  .enableHiveSupport()
  .getOrCreate()
  
  val df = spark.read
  .format("csv")
  .option("header",true)
  .option("inferSchema", true)
  .option("path","C:/Users/chnis/Downloads/Big Data/week12/windowdata.csv")
  .load()
  
  val myWindow = Window.partitionBy("country")
  .orderBy("weeknum")
  .rowsBetween(Window.unboundedPreceding, Window.currentRow)
  
  val df1 = df.withColumn("RunningTotal",sum("invoicevalue").over(myWindow))
  
  df1.show()
}