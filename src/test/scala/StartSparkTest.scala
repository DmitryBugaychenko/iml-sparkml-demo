package org.apache.spark.iml
import org.scalatest._
import flatspec._
import matchers._
import org.apache.spark.sql.SparkSession

/**
 * Simple example running local spark context.
 */
@Ignore
class StartSparkTest extends AnyFlatSpec with should.Matchers with WithSpark {

  "Spark" should "start context" in {
    val s = spark

    Thread.sleep(60000)
  }

}
