package org.apache.spark.iml

import com.microsoft.azure.synapse.ml.lightgbm.LightGBMClassifier
import org.apache.spark.ml.Pipeline
import org.apache.spark.ml.classification.{LogisticRegression, LogisticRegressionModel}
import org.apache.spark.ml.evaluation.BinaryClassificationEvaluator
import org.apache.spark.ml.feature.{SQLTransformer, StandardScaler, VectorAssembler}
import org.apache.spark.sql.DataFrame
import org.scalatest.flatspec._
import org.scalatest.matchers._

/**
 * Experimenting with different pipelines for the task
 */
class TrainModelTest extends AnyFlatSpec with should.Matchers with WithSpark with WithExpressions with WithDatasets{

  /**
   * Names of all columns with MCCs
   */
  private val mccNames: Array[String] = mccs.map(_._2)

  /**
   * Make sure train dataset has proper schema
   */
  "Trainer" should "create dataset" in {
    Set("client_id", "gender") ++ mccNames should be(trainDataset.schema.fieldNames.toSet)
  }

  /**
   * Pipeline for logistic regression
   */
  private lazy val logRegModel = {
    val pipeline = new Pipeline().setStages(Array(
      // Using SQLTransformer to add total and replace nulls with 0.0
      // Note the "SELECT *" part - Spark Serving adds system column with queryId to the stream and this column
      // must be preserved in the output.
      new SQLTransformer().setStatement(
        s"SELECT *, ${mccNames.map(x => s"NVL($x, 0.0) as ${x}_nn").mkString(", ")}, (${mccNames.map(x => s"NVL($x, 0.0)").mkString(" + ")}) as total FROM __THIS__"),
      // Combine all MCC spendings into feature vectors
      new VectorAssembler().setInputCols(mccNames.map(_ + "_nn") ++ Seq("total")).setOutputCol("features"),
      // Perform features standardization
      new StandardScaler().setInputCol("features").setOutputCol("features_sd"),
      // Use logistic regression classifier
      new LogisticRegression().setFeaturesCol("features_sd").setLabelCol("gender").setRegParam(0.01)
    ))

    // Train the model
    pipeline.fit(trainDataset)
  }

  "Trainer" should "fit logreg" in {
    val coefficient = (mccNames ++ Seq("total"))
      .zip(logRegModel.stages.last.asInstanceOf[LogisticRegressionModel].coefficients.toArray)
      .sortBy(_._2)

    // Check the cofficients for feature importance
    coefficient
      .foreach(System.out.println)

    // Make sure model have an appropriate quality
    val auc = new BinaryClassificationEvaluator().setLabelCol("gender").evaluate(logRegModel.transform(trainDataset))
    auc should be >= 0.8
  }

  "Trainer" should "persist logreg" in {
    // Save the model for future use in serving tests
    logRegModel.write.overwrite().save("./models/logReg")
  }

  /**
   * LightGBM model for the same task. OpenMP must be installed for this to work
   */
  private lazy val lightGbmModel = {
    val pipeline = new Pipeline().setStages(Array(
      new SQLTransformer().setStatement(
        s"SELECT *, ${mccNames.map(x => s"NVL($x, 0.0) as ${x}_nn").mkString(", ")}, (${mccNames.map(x => s"NVL($x, 0.0)").mkString(" + ")}) as total FROM __THIS__"),
      new VectorAssembler().setInputCols(mccNames.map(_ + "_nn") ++ Seq("total")).setOutputCol("features"),
      new StandardScaler().setInputCol("features").setOutputCol("features_sd"),
      new LightGBMClassifier().setFeaturesCol("features_sd").setLabelCol("gender").setLambdaL2(0.01)
    ))

    pipeline.fit(trainDataset)
  }

  "Trainer" should "fit lightgbm" in {
    val auc = new BinaryClassificationEvaluator()
      .setLabelCol("gender")
      .evaluate(lightGbmModel.transform(trainDataset))
    // We expect LightGBM to provide better quality :)
    auc should be >= 0.9
  }

  "Trainer" should "persist lightgbm" in {
    lightGbmModel.write.overwrite().save("./models/lightGbm")
  }

}

trait WithDatasets {
  lazy val trainUsers = TrainModelTest._trainUsers
  lazy val trainDataset = TrainModelTest._trainDataset

}


object TrainModelTest extends WithSpark with WithExpressions {
  /**
   * Train users with known gender
   */
  lazy val _trainUsers: DataFrame = sqlc.read
    .option("header", true)
    .option("delimiter", ",")
    .option("inferSchema", "true")
    .csv("./data/train.csv")
    .select("client_id", "gender")

  lazy val _trainDataset = _trainUsers.join(pivotedTransactions, Seq("client_id"), "inner").cache()
}