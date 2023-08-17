import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.log4j.Level
import org.apache.log4j.Logger
import org.apache.spark.sql.functions._

object GroupingAggregates extends App{
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
  
  df.groupBy("country", "InvoiceNo")
  .agg(sum("Quantity").as("TotalQuantity"),
       sum(expr("Quantity * UnitPrice")).as("TotalAmount")
      ).show()
  
  df.groupBy("country", "InvoiceNo")
  .agg(expr("sum(Quantity) as TotalQuantity"),
       expr("sum(Quantity * UnitPrice) as TotalAmount")
  ).show()
  
  df.createOrReplaceTempView("sales")
  
  spark.sql("""select country ,InvoiceNo, sum(Quantity) as TotalQuantity,sum(Quantity * UnitPrice) as TotalAmount
               from sales group by country ,InvoiceNo""").show
}