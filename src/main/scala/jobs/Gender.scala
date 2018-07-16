package jobs

import org.apache.spark.sql.SaveMode
import utils.My_Utils
import model.Logistic_Regression
import data_preprocess.Dataset
/**
  * Created by Wind on 2018/07/06.
  */

object Gender {
  def main(args: Array[String]): Unit = {
    // val job_time = "20180709"
    val job_time = args(0)
    val imei_features_table = "algo.yf_imei_features_app_install"
    val feature_dim: Int = 30000
    val model_save_dir = "/apps/recommend/models/wind/gender"
    val save_table_name: String = "algo.up_yf_gender_test"

    My_Utils.my_log("Initial Spark")
    val (sparkSession, job_date) = My_Utils.init_job(job_time)

    My_Utils.my_log("Getting gender labeled dataset")
    val gender_labeled_dataset = Dataset.get_gender_labeled_data(sparkSession, job_date)

    My_Utils.my_log("Geting all data with features including labeled and unlabeled set")
    val (dataset_labeled, dataset_unlabeled) = Dataset.get_dataset(sparkSession, gender_labeled_dataset, imei_features_table, feature_dim, job_date)

    My_Utils.my_log("Building lr model and training")
    val model = Logistic_Regression.build_model(sparkSession, dataset_labeled, dataset_unlabeled, classes_num = 2, model_refined_flag = true, model_save_dir, job_date)

    My_Utils.my_log("Predicting unlabeled data")
    val prediction = Logistic_Regression.predict(model, dataset_unlabeled)

    My_Utils.my_log("Union labelled and predicted data")
    val result = Logistic_Regression.union_train_and_prediction_data(dataset_labeled.map(v => (v._1, v._2.label)), prediction)

    My_Utils.my_log("Writing result to hive")
    import sparkSession.implicits._
    result.map(v => (v._1, v._2, job_date)).toDF("imei", "gender", "stat_date").write.partitionBy("stat_date").mode(SaveMode.Append).saveAsTable(save_table_name)
  }
}
