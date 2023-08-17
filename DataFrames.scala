import org.apache.spark.sql.SparkSession
import org.apache.spark.SparkConf
import org.apache.log4j.Level
import org.apache.log4j.Logger


object DataFrames extends App{
  Logger.getLogger("org").setLevel(Level.ERROR)
  val sparkConf=new SparkConf()
  sparkConf.set("spark.app.name","My Application 1")
  sparkConf.set("spark.master","local[2]")
  val spark=SparkSession.builder()
  .config(sparkConf)
  .getOrCreate()
  
  val ordersDf=spark.read
  .option("header",true)
  .option("inferSchema", true)
  .csv("C:/Users/chnis/Downloads/Big Data/week11/orders.csv")
  
  val gropuedOrdersDf=ordersDf.repartition(4).where("order_customer_id >10000")
  .select("order_id","order_customer_id")
  .groupBy("order_customer_id")
  .count()
  
  gropuedOrdersDf.show()
  
//  ordersDf.printSchema()
  
  
  
  spark.stop()
}