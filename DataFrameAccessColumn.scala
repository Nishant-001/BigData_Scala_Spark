import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.log4j.Level
import org.apache.log4j.Logger
import org.apache.spark.sql.functions._


object DataFrameAccessColumn extends App{
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
  
  import spark.implicits._
  
  ordersDf.select(column("order_id"),column("order_date"),expr("concat(order_status,'_STATUS')")).show(false)
  ordersDf.selectExpr("order_id","order_date","concat(order_status,'_STATUS')").show(false)
}