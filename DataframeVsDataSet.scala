import org.apache.spark.sql.SparkSession
import org.apache.spark.SparkConf
import org.apache.log4j.Level
import org.apache.log4j.Logger
import org.apache.commons.net.ntp.TimeStamp
import org.apache.spark.sql.Dataset
import org.apache.spark.sql.Row

case class OrdersData(order_id :Int,order_date :TimeStamp,order_customer_id: Int,order_status :String)
object DataframeVsDataSet extends App{
  Logger.getLogger("org").setLevel(Level.ERROR)
  val sparkConf=new SparkConf()
  sparkConf.set("spark.app.name","My Application 1")
  sparkConf.set("spark.master","local[2]")
  val spark=SparkSession.builder()
  .config(sparkConf)
  .getOrCreate()
  
//  <-----Shortcut way--------->
//  val ordersDf: Dataset[Row]=spark.read
//  .option("header",true)
//  .option("inferSchema", true)
//  .csv("C:/Users/chnis/Downloads/Big Data/week11/orders.csv")
  
//  <-----Standard way--------->
//   val ordersDf = spark.read
//  .format("csv")
//  .option("header",true)
//  .option("inferSchema", true)
//  .option("path","C:/Users/chnis/Downloads/Big Data/week11/orders.csv")
//  .load
  
//  val ordersDf = spark.read
//  .format("json")
//  .option("path","C:/Users/chnis/Downloads/Big Data/week11/players.json")
//  .option("mode","FAILFAST")
//  .load
  
  val ordersDf = spark.read
  .format("parquet")     //---->optional
  .option("path","C:/Users/chnis/Downloads/Big Data/week11/users.parquet")
  .load
  
//  import spark.implicits._
//  
//  val ordersDs=ordersDf.as[OrdersData]
//  
//  ordersDf.filter("order_id<10")
//  
//  ordersDs.filter(x => x.order_id<10).show()
  
//  val gropuedOrdersDf=ordersDf.repartition(4).where("order_customer_id >10000")
//  .select("order_id","order_customer_id")
//  .groupBy("order_customer_id")
//  .count()
  
//  gropuedOrdersDf.show()
  ordersDf.show(false)  //truncate false
  ordersDf.printSchema()
  
  
  
  spark.stop()
}