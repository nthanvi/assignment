package com.newday

import com.newday.Constants.{APP_NAME, MASTER}
import com.newday.config.Config
import com.newday.service.{MovieRatingService, MovieService, RatingService}
import org.apache.spark.SparkContext
import org.apache.spark.sql.SparkSession

// Todo Exception Handling
// Todo Externalize configuration
// Todo Comments and Logging
// command to run ./spark-submit  --class com.newday.Launcher --master local[*] /Users/nthanvi/movie/target/scala-2.12/assignment-1.0.jar
// keep files in same dir as spark-submit

object Launcher {
  def main(args: Array[String]) = {
    val sparkSession = SparkSession.builder
      .master(Config.getProperty(MASTER, "local[*]"))
      .appName("Something to do with movies")
      .getOrCreate();
    SparkContext.getOrCreate().setLogLevel("ERROR")
    new MovieService(sparkSession).process();
    new RatingService(sparkSession).process();
    new MovieRatingService(sparkSession).process();
  }
}
