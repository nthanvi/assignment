package com.newday.service

import com.newday.Constants.{OUTPUT_FILE, WRITE_FORMAT}
import com.newday.config.Config
import com.newday.service.io.{Format, Formatter, ReaderService, WriterService}
import com.newday.schema.{Movie, MovieRating, Rating, UsersFavourite}
import com.newday.service.MovieRatingService.{getMovieByRatings, getUsersFavourite}
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions.{asc, desc, rank}
import org.apache.spark.sql.{Dataset, Encoders, SparkSession, functions}

class MovieRatingService(session: SparkSession) {
  import session.implicits._

  def process(): Unit = {
    val movie = ReaderService.read(session, MovieService.writeFormatter).as[Movie]
    val rating = ReaderService.read(session, RatingService.writeFormatter).as[Rating]
    val movieRatings = getMovieByRatings(session, movie, rating);
    movieRatings.show(10);
    WriterService.write(movieRatings, MovieRatingService.writeFormatter);
    getUsersFavourite(session, movie, rating).show(10)
  }
}

object MovieRatingService {

  val id = "movie_ratings"

  val writeFormatter = Formatter(
    Encoders.product[MovieRating].schema,
    Seq("movieId"),
    Format.withName(Config.getProperty(s"${id}.${WRITE_FORMAT}",Format.PARQUET.toString)),
    Config.getProperty(s"${id}.${OUTPUT_FILE}", ""))

  def getMovieByRatings(session: SparkSession, movie: Dataset[Movie], rating: Dataset[Rating]): Dataset[MovieRating] = {
    import session.implicits._
    movie.join(rating, movie("movieId") === rating("movieId"), "inner")
      .groupBy(movie("movieId"), movie("title"), movie("genre"))
      .agg(functions.min("rating").alias("minimumRating"),
        functions.max("rating").alias("maximumRating"),
        functions.avg("rating").alias("averageRating")).as[MovieRating]
  }

  def getUsersFavourite(session: SparkSession, movie: Dataset[Movie], rating: Dataset[Rating]): Dataset[UsersFavourite] = {
    import session.implicits._
    val spec = Window.partitionBy(rating.col("userId"))
      .orderBy(desc("rating"), asc("movieId"));
    val ratingWithRank = rating.withColumn("rank", rank.over(spec));
    ratingWithRank.filter(ratingWithRank.col("rank") <= 3)
      .join(movie, movie("movieId") === ratingWithRank("movieId"), "inner")
      .select(ratingWithRank("userId"), movie("movieId"), movie("title"), ratingWithRank("rank"))
      .sort(ratingWithRank("userId"), movie("movieId"))
      .as[UsersFavourite]
  }
}
