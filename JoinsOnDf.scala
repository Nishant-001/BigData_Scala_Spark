import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.log4j.Level
import org.apache.log4j.Logger
import org.apache.spark.sql.functions._

object JoinsOnDf extends App{
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
  .option("inferSchema", true)
  .option("path","C:/Users/chnis/Downloads/Big Data/week12/orders.csv")
  .load()
  
  val customerDf = spark.read
  .format("csv")
  .option("header",true)
  .option("inferSchema", true)
  .option("path","C:/Users/chnis/Downloads/Big Data/week12/customers.csv")
  .load()
  
  val joincondition = ordersDf.col("order_customer_id") === customerDf.col("customer_id")
  val jointype = "outer"
  
  val joinDf = ordersDf.join(customerDf,joincondition,jointype)
  .drop(ordersDf.col("order_customer_id")).select("order_id","customer_id","customer_fname")
  .sort("order_id")
  .withColumn("order_id",expr("coalesce(order_id,-1)"))
  
  
  joinDf.show(100)
}