package com.newday.service

import com.newday.config.Config
import com.newday.schema.{Movie, MovieRating}
import com.newday.service.io.Format
import org.apache.spark.sql.Encoders
import org.scalatest.BeforeAndAfterAll
import org.scalatest.funsuite.AnyFunSuite

class MovieServiceTest  extends AnyFunSuite with BeforeAndAfterAll  {

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
  override protected def beforeAll(): Unit = Config.init(this.getClass.getResource("/env.properties").toString)
}
