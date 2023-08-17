import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.log4j.Level
import org.apache.log4j.Logger
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.DateType


object ListToDF extends App{
  Logger.getLogger("org").setLevel(Level.ERROR)
  val sparkConf=new SparkConf()
  sparkConf.set("spark.app.name","My Application 1")
  sparkConf.set("spark.master","local[2]")
  val spark=SparkSession.builder()
  .config(sparkConf)
  .enableHiveSupport()
  .getOrCreate()
  
  import spark.implicits._
  
  val mylist = List(
  (1,"2013-07-25",11599,"CLOSED"),
  (2,"2014-07-25",156,"PENDING_PAYMENT"),
  (3,"2013-07-25",11599,"COMPLETE"),
  (4,"2019-07-25",8827,"CLOSED")
  )
  
  val df = spark.createDataFrame(mylist).toDF("order_id","order_date","customer_id","status")
  .withColumn("order_date",unix_timestamp(col("order_date").cast(DateType)))
  .withColumn("newid", monotonically_increasing_id)
  .dropDuplicates("order_date", "customer_id")
  .drop("order_id")
  .sort("order_date")
  
  df.printSchema()
  df.show()
}