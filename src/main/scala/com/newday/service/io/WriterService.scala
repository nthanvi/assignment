package com.newday.service.io

import com.newday.log.Logging
import org.apache.spark.sql.{Column, DataFrame, Dataset}

object WriterService extends Logging {
  def write(df: Dataset[_], formatter:Formatter): Unit = {
    val keys: Seq[Column] = formatter.partitionKey.map(x => df.col(x)).seq
    formatter.format match {
      case _ => {
        df.repartition(40, keys:_*).write.mode("overwrite").parquet(formatter.path)
      };
    }
  }
}
