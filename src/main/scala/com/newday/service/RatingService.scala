package com.newday.service

import com.newday.Constants.{INPUT_FILE, OUTPUT_FILE, READ_FORMAT, WRITE_FORMAT}
import com.newday.config.Config
import com.newday.service.io.{Format, Formatter, ReaderService, WriterService}
import com.newday.schema.{Movie, Rating}
import com.newday.service.RatingService.{readformatter, writeFormatter}
import org.apache.spark.sql.{Encoders, SparkSession}

class RatingService(session:SparkSession) {
  import  session.implicits._
  val id = "ratings"

  def process(): Unit = {
    val sourceDF = ReaderService.read(session, readformatter).as[Rating]
    WriterService.write(sourceDF, writeFormatter);
  }
}

object RatingService {
 val id = "ratings";
  val readformatter =  Formatter(
    Encoders.product[Rating].schema,
    Seq("movieId"),
    Format.withName(Config.getProperty(s"${id}.${READ_FORMAT}",Format.PARQUET.toString)),
    Config.getProperty(s"${id}.${INPUT_FILE}", ""))

  val writeFormatter = Formatter(
    Encoders.product[Rating].schema,
    Seq("movieId"),
    Format.withName(Config.getProperty(s"${id}.${WRITE_FORMAT}",Format.PARQUET.toString)),
    Config.getProperty(s"${id}.${OUTPUT_FILE}", ""))
}