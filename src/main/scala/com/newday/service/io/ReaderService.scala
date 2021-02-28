package com.newday.service.io

import com.newday.log.Logging
import Format.CSV
import org.apache.spark.sql.{DataFrame, Encoder, SparkSession}

object ReaderService extends Logging {
  def read(session: SparkSession, formatter: Formatter): DataFrame= {
    formatter.format match {
      case CSV => {
        session.sqlContext.read.option("delimiter", "::").schema(formatter.schema).csv(formatter.path)
      };
      case _ => {
        session.sqlContext.read.schema(formatter.schema).parquet(formatter.path)
      };
    }
  }
}
