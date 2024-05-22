package org.apache.spark.iml

import com.ibm.icu.text.Transliterator
import org.apache.spark.sql.Encoder
import org.apache.spark.sql.catalyst.encoders.ExpressionEncoder
import org.scalatest.flatspec._
import org.scalatest.matchers._

/**
 * In this test we are looking at the dataset
 */
class ExploreDataTest extends AnyFlatSpec with should.Matchers with WithSpark with WithExpressions {

  /**
   * MMC codes explanations are given in russian, in order to use them as column names
   * we perform transliteration.
   */
  "Explore" should "support transliteration" in {
    import com.ibm.icu.text.Transliterator

    import java.util
    val input = "Привет генеральные подрядчики"

    val availableIDs: util.Enumeration[String] = Transliterator.getAvailableIDs

    while (availableIDs.hasMoreElements) {
      val id = availableIDs.nextElement()
      val toLatinTrans = Transliterator.getInstance(id)
      val result = toLatinTrans.transliterate(input)
      if(result != input) System.out.println(id + " : " + input + " -> " + result)
    }
  }

  /**
   * Check the column names
   */
  "Explore" should "produce column names" in {
    mccs.map(_._2).foreach(System.out.println)
  }

  /**
   * Check the schema
   */
  "Explore" should "should infer schema" in {
    transactions.printSchema()
    transactions.show(10, truncate = false)
  }

  /**
   * Checl the query
   */
  "Explore" should "should pivot" in {
    pivotedTransactions.printSchema()
    pivotedTransactions.show(10, truncate = false)

    transactions.select("client_id").distinct().count() should be(pivotedTransactions.select("client_id").distinct().count())
  }
}

trait WithExpressions {
  /**
   * Available MCC codes in format code -> meaning. Meaning is transliterated and escaped to
   * become valid column name
   */
  lazy val mccs: Array[(String, String)] = ExploreDataTest._mccs

  /**
   * Query used to produce features for the clients based on transactions.
   * Note __THIS__ placeholder added for compatibility with SparkML SQLTransformer.
   */
  lazy val transactionQuery =
    s"""
       |SELECT client_id, ${mccs.map(x => s"SUM(${x._2}) AS ${x._2}").mkString(", ")}
       |FROM (SELECT client_id, mcc_code, amount FROM __THIS__)
       |PIVOT(SUM(amount) FOR mcc_code in ${mccs.map(x => s"'${x._1}' AS ${x._2}").mkString("(", ", ", " )")})
       |GROUP BY client_id;""".stripMargin

  lazy val transactions = ExploreDataTest._transactions

  lazy val pivotedTransactions = ExploreDataTest._pivotedTransactions


}

object ExploreDataTest extends WithSpark with WithExpressions {
  lazy implicit val encoder : Encoder[String] = ExpressionEncoder()

  /**
   * MCCs map
   */
  lazy val _mccs = {
    val mcc = sqlc.read
      .option("header", true)
      .option("delimiter", ";")
      .csv("./data/mcc_codes.csv")

    val toLatinTrans = Transliterator.getInstance("Russian-Latin/BGN")

    mcc
      .select(mcc("mcc_code").as[String], mcc("mcc_description").as[String])
      .collect()
      .map(x => x._1 ->
        toLatinTrans.transliterate(x._2)
          .replaceAll("[\\p{P}|\\p{Z}|ʹ]+", "_"))
  }

  /**
   * Transactions dataset
   */
  lazy val _transactions = sqlc.read
    .option("header", true)
    .option("inferSchema", true)
    .csv("./data/transactions.csv")

  /**
   * Transactions dataset pivoted by MCC code
   */
  lazy val _pivotedTransactions = {
    _transactions.createTempView("transactions")

    sqlc.sql(transactionQuery.replace("__THIS__", "transactions"))
  }
}