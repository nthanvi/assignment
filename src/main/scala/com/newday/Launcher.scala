package com.newday

import com.newday.Constants.{APP_NAME, MASTER}
import com.newday.config.Config
import com.newday.service.{MovieRatingService, MovieService, RatingService}
import org.apache.spark.SparkContext
import org.apache.spark.sql.SparkSession

// Todo Exception Handling
// Todo Externalize configuration
// Todo Comments and Logging

object Launcher {
  def main(args: Array[String]) = {
    Config.init(args(0))

    val sparkSession = SparkSession.builder
      .appName("Something to do with movies")
      .getOrCreate();
    // Todo  check arguments etc
    print("Running with " + args(0));
    SparkContext.getOrCreate().setLogLevel("ERROR")
    new MovieService(sparkSession).process();
    new RatingService(sparkSession).process();
    new MovieRatingService(sparkSession).process();
  }
}
