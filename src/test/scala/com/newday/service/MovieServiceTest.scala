package com.newday.service

import com.newday.schema.{Movie, MovieRating}
import com.newday.service.io.Format
import org.apache.spark.sql.Encoders
import org.scalatest.funsuite.AnyFunSuite

class MovieServiceTest  extends AnyFunSuite {

  test ("readFormatter") {
    assert(MovieService.readformatter.schema == Encoders.product[Movie].schema);
    assert(MovieService.readformatter.format == Format.CSV)
    assert(MovieService.readformatter.partitionKey == Seq("movieId"))
    assert(MovieService.readformatter.path == "movies.dat")
  }

  test ("writeFormatter") {
    assert(MovieService.writeFormatter.schema == Encoders.product[Movie].schema);
    assert(MovieService.writeFormatter.format == Format.PARQUET)
    assert(MovieService.writeFormatter.partitionKey == Seq("movieId"))
    assert(MovieService.writeFormatter.path == "movies_out")
  }
}
