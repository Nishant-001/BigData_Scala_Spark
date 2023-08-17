import org.apache.spark.sql.SparkSession
import org.apache.spark.SparkConf
import org.apache.log4j.Level
import org.apache.log4j.Logger
import org.apache.commons.net.ntp.TimeStamp
import org.apache.spark.sql.Dataset
import org.apache.spark.sql.Row
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.types.IntegerType
import org.apache.spark.sql.types.StructField
import org.apache.spark.sql.types.TimestampType
import org.apache.spark.sql.types.StringType


object SparkSchemaExmaples extends App{
  case class OrdersData(order_id :Int,order_date :TimeStamp,order_customer_id: Int,order_status :String)
  Logger.getLogger("org").setLevel(Level.ERROR)
  val sparkConf=new SparkConf()
  sparkConf.set("spark.app.name","My Application 1")
  sparkConf.set("spark.master","local[2]")
  val spark=SparkSession.builder()
  .config(sparkConf)
  .getOrCreate()

//  val ordersSchema = StructType(List(                // Programatic way
//      StructField("orderid",IntegerType,true),
//      StructField("orderdate",TimestampType),
//      StructField("customerid",IntegerType),
//      StructField("status",StringType)
//      ))
      
   val  ordersSchemaDDL = "orderid Int, orderdate String, custid Int,ordersStatus String"
   val ordersDf = spark.read
  .format("csv")
  .option("header",true)
  .schema(ordersSchemaDDL)
  .option("path","C:/Users/chnis/Downloads/Big Data/week11/orders.csv")
  .load
  
    import spark.implicits._
  
  val orderDS = ordersDf.as[OrdersData]
ordersDf.printSchema
ordersDf.show
  
  
  
  spark.stop()
}