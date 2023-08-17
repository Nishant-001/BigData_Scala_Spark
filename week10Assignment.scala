import org.apache.spark.SparkContext
import org.apache.log4j.Level
import org.apache.log4j.Logger


object week10Assignment extends App{
  Logger.getLogger("org").setLevel(Level.ERROR)
  val sc=new SparkContext("local[*]","customerdata")
  val chapterDataRDD =sc.textFile("C:/Users/chnis/Downloads/Big Data/week10/chapters.csv").map(x => {
  val chapterDataFields = x.split(",")
    (chapterDataFields(0).toInt,chapterDataFields(1).toInt)
  })
  val viewDataRDD =sc.textFile("C:/Users/chnis/Downloads/Big Data/week10/views*.csv")
  .map(x => {
    (x.split(",")(0).toInt, x.split(",")(1).toInt)
  })
  val titlesDataRDD =sc.textFile("C:/Users/chnis/Downloads/Big Data/week10/titles.csv").map( x =>(x.split(",")(0).toInt, x.split(",")(1)))

  
  val chapterCountRDD = chapterDataRDD.map(x =>(x._2,1)).reduceByKey((x,y) => x + y)
  val viewDataDistinctRDD = viewDataRDD.distinct()
  val flippedviewDataRDD = viewDataDistinctRDD.map(x =>(x._2,x._1))
  val joinedRDD = flippedviewDataRDD.join (chapterDataRDD)
  val pairRDD = joinedRDD.map( x => ((x._2._1, x._2._2),1))
  val userPerCourseViewRDD = pairRDD.reduceByKey(_ + _)
  val courseViewsCountRDD = userPerCourseViewRDD.map( x =>(x._1._2,x._2))
  val newJoinedRDD =courseViewsCountRDD.join(chapterCountRDD)
  val CourseCompletionpercentRDD = newJoinedRDD.mapValues(x => (x._1.toDouble/x._2))
  val formattedpercentageRDD =CourseCompletionpercentRDD.mapValues(x =>f"$x%01.5f".toDouble)
  val scoresRDD = formattedpercentageRDD.mapValues (x => {
    if(x >= 0.9) 10l
    else if(x >= 0.5 && x < 0.9) 4l
    else if(x >= 0.25 && x < 0.5) 2l
    else 0l
    })
    
    val totalScorePerCourseRDD =scoresRDD.reduceByKey((V1,V2) => V1 + V2)
    val title_score_joinedRDD =totalScorePerCourseRDD.join(titlesDataRDD).map( x =>(x._2._1, x._2._2))

    val popularCoursesRDD =title_score_joinedRDD.sortByKey(false)
    popularCoursesRDD.collect.foreach(println)
}