package com.newday.service

import com.newday.Constants.{INPUT_FILE, OUTPUT_FILE, READ_FORMAT, WRITE_FORMAT}
import com.newday.config.Config
import com.newday.service.io.{Format, Formatter, ReaderService, WriterService}
import com.newday.schema.Movie
import com.newday.service.MovieService.{readformatter, writeFormatter}
import org.apache.spark.sql.{Encoders, SparkSession}

class MovieService(session:SparkSession) {
  import  session.implicits._
  def process(): Unit = {
    val sourceDF = ReaderService.read(session, readformatter).as[Movie];
    WriterService.write(sourceDF, writeFormatter);
  }
}

object MovieService {
  val id = "movies"

  val readformatter = Formatter(
    Encoders.product[Movie].schema,
    Seq("movieId"),
    Format.withName(Config.getProperty(s"${id}.${READ_FORMAT}",Format.PARQUET.toString)),
    Config.getProperty(s"${id}.${INPUT_FILE}", ""))

  val writeFormatter = Formatter(
    Encoders.product[Movie].schema,
    Seq("movieId"),
    Format.withName(Config.getProperty(s"${id}.${WRITE_FORMAT}",Format.PARQUET.toString)),
    Config.getProperty(s"${id}.${OUTPUT_FILE}", ""))
}
