package jobs

import model.Logistic_Regression
import org.apache.spark.sql.SaveMode

import utils.My_Utils
import data_preprocess.Dataset


object Age {
  def main(args: Array[String]): Unit = {
    // val job_time = "20180709"
    val job_time = args(0)
    val imei_features_table = "algo.yf_imei_features_app_install"
    val feature_dim: Int = 30000
    val model_save_dir = "/apps/recommend/models/wind/age"
    val save_table_name: String = "algo.up_yf_gender_test"
    val save_flag = false

    My_Utils.my_log("Initial Spark")
    val (sparkSession, job_date) = My_Utils.init_job(job_time)

    My_Utils.my_log("Getting age labeled dataset")
    val age_labeled_dataset = Dataset.get_age_labeled_data(sparkSession, job_date)

    My_Utils.my_log("Geting all data with features including labeled and unlabeled set")
    val (dataset_labeled, dataset_unlabeled) = Dataset.get_dataset(sparkSession, age_labeled_dataset, imei_features_table, feature_dim, job_date)

    My_Utils.my_log("Building lr model and training")
    val model = Logistic_Regression.build_model(sparkSession, dataset_labeled, dataset_unlabeled, classes_num = 5, model_refined_flag = false, model_save_dir, job_date)

    My_Utils.my_log("Predicting unlabeled data")
    val prediction = Logistic_Regression.predict(model, dataset_unlabeled)

    My_Utils.my_log("Union labelled and predicted data")
    val result = Logistic_Regression.union_train_and_prediction_data(dataset_labeled.map(v => (v._1, v._2.label)), prediction)

    if (save_flag) {
      My_Utils.my_log("Writing result to hive")
      import sparkSession.implicits._
      result.map(v => (v._1, v._2, job_date)).toDF("imei", "age", "stat_date").write.partitionBy("stat_date").mode(SaveMode.Append).saveAsTable(save_table_name)
    }
  }
}
