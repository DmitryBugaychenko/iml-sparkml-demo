package org.apache.spark.iml

import org.apache.spark.sql.catalyst.encoders.ExpressionEncoder
import org.apache.spark.sql.{Encoder, SparkSession}

trait WithSpark {
  lazy val spark = WithSpark._spark
  lazy val sqlc = WithSpark._sqlc
}

object WithSpark {
  lazy val _spark = SparkSession.builder
    .appName("Simple Application")
    .master("local[4]")
    .config("spark.sql.shuffle.partitions", 4)
    .getOrCreate()

  lazy val _sqlc = _spark.sqlContext
}
