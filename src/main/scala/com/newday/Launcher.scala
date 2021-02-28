package com.newday

import com.newday.Constants.{APP_NAME, MASTER}
import com.newday.config.Config
import com.newday.service.{MovieRatingService, MovieService, RatingService}
import org.apache.spark.SparkContext
import org.apache.spark.sql.SparkSession

// Todo Exception Handling
// externalize configuration
// command to run ./spark-submit  --class Launcher --master local[*] /Users/nthanvi/sources/newday/movie/target/scala-2.12/movie-1.0.jar
object Launcher {
  def main(args: Array[String]) = {
    val sparkSession = SparkSession.builder
      .master(Config.getProperty(MASTER, "local[*]"))
      .appName("Something to do with movies")
      .config("spark.ui.enabled", true)
      .getOrCreate();
    SparkContext.getOrCreate().setLogLevel("ERROR")
    new MovieService(sparkSession).process();
    new RatingService(sparkSession).process();
    new MovieRatingService(sparkSession).process();
  }
}
