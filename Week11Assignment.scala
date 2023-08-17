import org.apache.log4j.Level
import org.apache.log4j.Logger
import org.apache.spark.SparkConf
import org.apache.spark.sql.SaveMode
import org.apache.spark.sql.SparkSession
//import org.apache.spark.sql._

object Week11Assignment extends App{
  Logger.getLogger("org").setLevel(Level.ERROR)
  val sparkConf=new SparkConf()
  sparkConf.set("spark.app.name","My Application 1")
  sparkConf.set("spark.master","local[2]")
  val spark=SparkSession.builder()
  .config(sparkConf)
  .getOrCreate()
  
  val DDLString = "country String, weeknum Int, numinvoices Int, totalquantity Int, invoicevalue Double"  
  val input = spark.read
  .format("csv")
  .schema(DDLString)
  .option("path","C:/Users/chnis/Downloads/Big Data/week11/windowdata.csv")
  .load
  
  
  input.write
  .format("avro")
  .partitionBy("country")
  .mode(SaveMode.Overwrite)
  .option("path","C:/Users/chnis/Downloads/Big Data/week11/windowdata_output_avro")
  .save()
}