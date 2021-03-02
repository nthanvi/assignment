package com.newday.service

import com.newday.Constants.MASTER
import com.newday.config.Config
import com.newday.schema.{Movie, MovieRating, Rating}
import com.newday.service.io.Format
import org.apache.spark.sql.{Encoders, SparkSession}
import org.scalatest.BeforeAndAfterAll
import org.scalatest.funsuite.AnyFunSuite

class MovieRatingServiceTest extends AnyFunSuite with BeforeAndAfterAll {

  test ("writeFormatter") {
    assert(MovieRatingService.writeFormatter.schema == Encoders.product[MovieRating].schema);
    assert(MovieRatingService.writeFormatter.format == Format.PARQUET)
    assert(MovieRatingService.writeFormatter.partitionKey == Seq("movieId"))
    assert(MovieRatingService.writeFormatter.path == "movie_ratings")
  }

  test("getMovieByRatings") {
    val sparkSession = SparkSession.builder
      .master(Config.getProperty(MASTER, "local[*]"))
      .appName("Something to do with movies")
      .config("spark.ui.enabled", true)
      .getOrCreate();

    import sparkSession.implicits._

    val movie = Seq(Movie(1, "hello", "Comedy"),
      Movie(2, "bye", "Comedy"),
      Movie(3, "good one", "Comedy"),
      Movie(4, "bad one", "Comedy")
    )

    val ratings = Seq(Rating(101, 1, 5, 1),
      Rating(101, 2, 4, 1),
      Rating(101, 3, 1, 1),
      Rating(201, 1, 5, 1),
      Rating(201, 4, 2, 1),
      Rating(303, 2, 3, 1)
    )

    val movieRatings = Seq(MovieRating(1, "hello", "Comedy", 5, 5, 5.0),
      MovieRating(3, "good one", "Comedy", 1, 1, 1.0),
      MovieRating(4, "bad one", "Comedy", 2, 2, 2.0),
      MovieRating(2, "bye", "Comedy", 3, 4, 3.5)
    )

    val movieRating = MovieRatingService.getMovieByRatings(sparkSession,
      sparkSession.sqlContext.sparkContext.parallelize(movie).toDS(),
      sparkSession.sqlContext.sparkContext.parallelize(ratings).toDS());

    assert(movieRatings == movieRating.collect().toSeq)
  }

  override protected def beforeAll(): Unit = Config.init(this.getClass.getResource("/env.properties").toString)
}
