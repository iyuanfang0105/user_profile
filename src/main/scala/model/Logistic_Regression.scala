package model

import org.apache.spark.mllib.classification.{LogisticRegressionModel, LogisticRegressionWithLBFGS}
import org.apache.spark.mllib.evaluation.BinaryClassificationMetrics
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import utils.My_Utils

object Logistic_Regression {
  def build_model(sparkSession: SparkSession,
                  dataset_labeled: RDD[(String, LabeledPoint)],
                  dataset_unlabeled: RDD[(String, LabeledPoint)],
                  classes_num: Int,
                  model_refined_flag: Boolean,
                  model_save_dir: String,
                  job_date: String): LogisticRegressionModel = {
    val rdd_temp: Array[RDD[(String, LabeledPoint)]] = dataset_labeled.randomSplit(Array(0.8, 0.2))
    val train_rdd: RDD[(String, LabeledPoint)] = rdd_temp(0).cache()
    val valid_rdd: RDD[(String, LabeledPoint)] = rdd_temp(1).cache()
    printf("\n====>>>> trainset: %d\n", train_rdd.count())
    My_Utils.numerical_label_distribute(train_rdd.map(v => (v._1, v._2.label)))

    var model: LogisticRegressionModel = new LogisticRegressionWithLBFGS().setNumClasses(classes_num).run(train_rdd.map(_._2))
    val defalut_threshold: Double = model.getThreshold.get
    printf("\n====>>>> default threshold: %.2f", defalut_threshold)

    val valid_result: RDD[(String, (Double, Double))] = valid_rdd.map(v => (v._1, (model.predict(v._2.features), v._2.label)))

    if (classes_num == 2) {
      val (auc, accu) = My_Utils.ROC(valid_result.map(_._2))
    }

    if (classes_num > 2) {
      My_Utils.confusion_matrix(valid_result.map(_._2))
    }

    if (classes_num == 2 && model_refined_flag){
      val model_refined = lr_model_refine(model, valid_rdd)
      val valid_result_refined: RDD[(String, (Double, Double))] = valid_rdd.map(v => (v._1, (model_refined.predict(v._2.features), v._2.label)))
      val (auc_refined, accu_refined) = My_Utils.ROC(valid_result_refined.map(_._2))
      model = model_refined
    }

    My_Utils.save_model(sparkSession, model, model_save_dir, job_date)
    model
  }

  def predict(model: LogisticRegressionModel, dataset_unlabeled: RDD[(String, LabeledPoint)]): RDD[(String, Double)] = {
    val result = dataset_unlabeled.map(v => {
      val imei = v._1
      val pred_label = model.predict(v._2.features)
      (imei, pred_label)
    })
    My_Utils.numerical_label_distribute(result)
    result
  }

  def union_train_and_prediction_data(trainset: RDD[(String, Double)], predictset: RDD[(String, Double)]): RDD[(String, Double)] = {
    val result = trainset.union(predictset)
    My_Utils.numerical_label_distribute(result)
    result
  }

  def lr_model_refine(model: LogisticRegressionModel, valid_set: RDD[(String, LabeledPoint)]): LogisticRegressionModel = {
    model.clearThreshold()
    val valid_result = valid_set.map(v => (v._1, (model.predict(v._2.features), v._2.label)))
    val binaryClassificationMetrics = new BinaryClassificationMetrics(valid_result.map(_._2))
    val fMeasure = binaryClassificationMetrics.fMeasureByThreshold()
    val best_threshold = fMeasure.reduce((a, b) => {
      if (a._2 < b._2) b else a
    })._1

    val default_thred = 0.5
    if (best_threshold >= default_thred - 0.05 && best_threshold <= default_thred + 0.05) {
      printf("\n====>>>> using best threshold: %.2f\n", best_threshold)
      model.setThreshold(best_threshold)
    } else {
      printf("\n====>>>> using default threshold: %.2f\n", default_thred)
      model.setThreshold(default_thred)
    }
    model
  }
}
