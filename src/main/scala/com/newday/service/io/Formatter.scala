package com.newday.service.io

import org.apache.spark.sql.types.StructType


case class Formatter(schema:StructType, partitionKey: Seq[String], format:Format.Value, path:String)

object Format extends Enumeration {
  val CSV, PARQUET = Value
}

