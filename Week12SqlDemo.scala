import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.log4j.Level
import org.apache.log4j.Logger


object Week12SqlDemo extends App{
  Logger.getLogger("org").setLevel(Level.ERROR)
  val sparkConf=new SparkConf()
  sparkConf.set("spark.app.name","My Application 1")
  sparkConf.set("spark.master","local[2]")
  val spark=SparkSession.builder()
  .config(sparkConf)
  .getOrCreate()
  
  val logDf = spark.read
  .format("csv")
  .option("header",true)
  .option("inferSchema", true)
  .option("path","C:/Users/chnis/Downloads/Big Data/week12/biglog.txt")
  .load()
  
  logDf.createOrReplaceTempView("my_new_logging_table")
  
//  val df1 = spark.sql(""" select level,date_format(datetime,'MMMM') as month,count(1) as total
//                from  my_new_logging_table group by level,month """)
//                
//  val df2 = spark.sql("""select level , date_format(datetime,'MMMM') as month,
//                         cast(first(date_format(datetime, 'M'))as int) as monthnum, count(1) as total
//                         from my_new_logging_table group by level,month order by monthnum, level """)
                         
                         
//  val result=df2.drop("monthnum")
//  
//  result.show()
   val columns = List("january","February","March","April","May","June","July","August","September","October","November","December")
   val df2 = spark.sql("""select level , date_format(datetime,'MMMM') as month,
                         cast(date_format(datetime, 'M')as int) as monthnum
                         from my_new_logging_table """).groupBy("level").pivot("month",columns).count().show()
}