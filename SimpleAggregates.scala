import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.log4j.Level
import org.apache.log4j.Logger
import org.apache.spark.sql.functions._


object SimpleAggregates extends App{
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
  .option("path","C:/Users/chnis/Downloads/Big Data/week12/order_data.csv")
  .load()
  
  df.select(
      count("*") as("RowCount"),
      sum("Quantity") as ("TotalQuantity"),
      avg("UnitPrice") as ("AvgPrice"),
      countDistinct("InvoiceNo") as ("DistinctInvoiceCount")
      ).show
  
      df.selectExpr(
      "count(*) as RowCount",
      "sum(Quantity) as TotalQuantity",
      "avg(UnitPrice) as AvgPrice",
      "count(Distinct(InvoiceNo)) as DistinctInvoiceCount"
      ).show
      
      df.createOrReplaceTempView("sales")
      
      spark.sql(" select count(*),sum(Quantity),avg(UnitPrice),count(distinct(InvoiceNo)) from sales").show
//  df.show()
}