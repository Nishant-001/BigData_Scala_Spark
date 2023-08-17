import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.log4j.Level
import org.apache.log4j.Logger
import org.apache.spark.sql.Dataset
import org.apache.spark.sql.Row
import org.apache.spark.sql.functions._


object AddColumnUsingHigherOrderFunctionsDataFrames extends App{
  Logger.getLogger("org").setLevel(Level.ERROR)
  
  def ageCheck(age :Int):String = {
    if(age >18) "y" else "N"
  }
  val sparkConf=new SparkConf()
  sparkConf.set("spark.app.name","My Application 1")
  sparkConf.set("spark.master","local[2]")
  val spark=SparkSession.builder()
  .config(sparkConf)
  .enableHiveSupport()
  .getOrCreate()
  
  val input = spark.read
  .format("csv")
  .option("inferSchema", true)
  .option("path","C:/Users/chnis/Downloads/Big Data/week12/dataset1")
  .load()
  
  val df: Dataset[Row] = input.toDF("name","age","city")
  
//  case class Person (name:String ,age :Int, city: String)
//  import spark.implicits._
//  val ds = df.as[Person]
  
//  val parseAgeFunction = udf(ageCheck(_:Int):String)
//  val df2 = df.withColumn("adult", parseAgeFunction(col("age")))
  
    spark.udf.register("parseAgeFunction",ageCheck(_:Int):String)
    val df2 = df.withColumn("adult", expr("parseAgeFunction(age)"))
    
    df2.show
    
    df.createOrReplaceTempView("PersonTable")
    spark.sql("select name ,age,city , parseAgeFunction(age) as adult from PersonTable").show()
    
//  input.printSchema()
}