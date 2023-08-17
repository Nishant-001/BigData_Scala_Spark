import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.log4j.Level
import org.apache.log4j.Logger
import org.apache.spark.sql.SaveMode


object Week12Assignment extends App{
  Logger.getLogger("org").setLevel(Level.ERROR)
  val sparkConf=new SparkConf()
  sparkConf.set("spark.app.name","My Application 1")
  sparkConf.set("spark.master","local[2]")
  val spark=SparkSession.builder()
  .config(sparkConf)
  .getOrCreate()
  
  val deptDf = spark.read
  .format("json")
  .option("path","C:/Users/chnis/Downloads/Big Data/week12 -Spark4(Structured Api 2)/dept.json")
  .load()
  

  
  val empDf = spark.read
  .format("json")
  .option("path","C:/Users/chnis/Downloads/Big Data/week12 -Spark4(Structured Api 2)/employee.json")
  .load()
  
  val df2 = empDf.withColumnRenamed("deptid", "empdeptid")
  
  deptDf.createOrReplaceTempView("dept")
  df2.createOrReplaceTempView("emp")
  
  
  val df3 = spark.sql(""" select deptName,first(deptid) as deptid, count(1) as empcount 
                from dept join emp where emp.empdeptid=dept.deptid group by deptName """)
                
   df3.write
  .format("csv")
  .mode(SaveMode.Overwrite)
  .option("path", "C:/Users/chnis/Downloads/Big Data/week12 -Spark4(Structured Api 2)/csv_output")
  .save()
}