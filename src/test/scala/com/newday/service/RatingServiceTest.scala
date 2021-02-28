package com.newday.service

import com.newday.schema.{Movie, Rating}
import com.newday.service.io.Format
import org.apache.spark.sql.Encoders
import org.scalatest.funsuite.AnyFunSuite


class RatingServiceTest extends AnyFunSuite {

  test ("readFormatter") {
    assert(RatingService.readformatter.schema == Encoders.product[Rating].schema);
    assert(RatingService.readformatter.format == Format.CSV)
    assert(RatingService.readformatter.partitionKey == Seq("movieId"))
    assert(RatingService.readformatter.path == "ratings.dat")
  }

  test ("writeFormatter") {
    assert(RatingService.writeFormatter.schema == Encoders.product[Rating].schema);
    assert(RatingService.writeFormatter.format == Format.PARQUET)
    assert(RatingService.writeFormatter.partitionKey == Seq("movieId"))
    assert(RatingService.writeFormatter.path == "ratings_out")
  }
}

