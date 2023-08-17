import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.log4j.Level
import org.apache.log4j.Logger
import org.apache.spark.sql.SaveMode


object Week11Assignment2 extends App{
  Logger.getLogger("org").setLevel(Level.ERROR)
  val sparkConf=new SparkConf()
  sparkConf.set("spark.app.name","My Application 1")
  sparkConf.set("spark.master","local[2]")
  val spark=SparkSession.builder()
  .config(sparkConf)
  .getOrCreate()
  
  import spark.implicits._
  
  val windowDataDF = spark.sparkContext.textFile("C:/Users/chnis/Downloads/Big Data/week11/windowdata.csv")
  .map(_.split(","))
  .map(e =>(e(0), e(1).trim.toInt, e(2).trim.toInt,e(3).trim.toInt, e(4))).toDF()
  .repartition(8)
  
  windowDataDF.write
  .format("json")
  .mode(SaveMode.Overwrite)
  .option("path", "C:/Users/chnis/Downloads/Big Data/week11/windowData_jsonoutput")
  .save()
  
  //windowDataDF.show()spark.stop()scala.io.StdIn.readLine()}
}