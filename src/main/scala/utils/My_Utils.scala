package utils

import java.text.SimpleDateFormat
import java.util.Calendar

import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.log4j.{Level, Logger}
import org.apache.spark.mllib.classification.LogisticRegressionModel
import org.apache.spark.mllib.evaluation.{BinaryClassificationMetrics, MulticlassMetrics}
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, SparkSession}

import scala.collection.mutable.ArrayBuffer

object My_Utils {
  def ROC(valid_result: RDD[(Double, Double)]): (Double, Double) = {
    val binary_class_metrics = new BinaryClassificationMetrics(valid_result)

    val roc = binary_class_metrics.roc()
    val au_roc = binary_class_metrics.areaUnderROC()
    val accuracy = valid_result.filter(v => v._1 == v._2).count() * 1.0 / valid_result.count()
    printf("\n====>>>> AUROC: %.4f, Accuracy: %.4f\n", au_roc, accuracy)
    (au_roc, accuracy)
  }

  def save_model(sparkSession: SparkSession, model: LogisticRegressionModel, save_dir: String, job_date: String) = {
    val fs = FileSystem.get(new org.apache.hadoop.conf.Configuration())
    val p = new Path(save_dir + "/" + job_date)
    if (fs.exists(p)) {
      printf("\n====>>>> model file is alread exists! delete it! %s", p.toString)
      fs.delete(p, true)
    }
    printf("\n====>>>> save model to %s", p.toString)
    model.save(sparkSession.sparkContext, p.toString)
  }

  def init_job(job_time: String): (SparkSession, String) = {
    val sparkSession =  SparkSession.builder().enableHiveSupport().getOrCreate()

    Logger.getLogger("org").setLevel(Level.ERROR)
    Logger.getLogger("org.apache.spark").setLevel(Level.ERROR)
    Logger.getLogger("akka").setLevel(Level.ERROR)
    Logger.getRootLogger().setLevel(Level.ERROR)

    val year: Int = job_time.substring(0, 4).trim.toInt
    val month: Int = job_time.substring(4, 6).trim.toInt
    val day: Int = job_time.substring(6, 8).trim.toInt
    val calendar: Calendar = Calendar.getInstance
    calendar.set(year, month - 1, day)
    val job_date: String = new SimpleDateFormat("yyyyMMdd").format(calendar.getTime)
    (sparkSession, job_date)
  }

  def my_log(g: String) = {
    printf("\n\n++++++++\n++++++++ %s\n++++++++\n\n", g)
  }

  def numerical_label_distribute(labeled_data: RDD[(String, Double)]) = {
    val labeled_data_count = labeled_data.count()
    labeled_data.map(v => (v._2, 1)).reduceByKey(_+_).collect().foreach(f = v => {
      printf("\n====>>>> label_%s: %d, all: %d, ratio: %.4f\n", v._1, v._2, labeled_data_count, v._2 * 1.0 / labeled_data_count)
    })
  }

  def string_label_distribute(labeled_data: RDD[(String, String)]) = {
    val labeled_data_count = labeled_data.count()
    labeled_data.map(v => (v._2, 1)).reduceByKey(_+_).collect().foreach(v => {
      printf("\n====>>>> label_%s: %d, all: %d, ratio: %.2f\n", v._1, v._2, labeled_data_count, v._2 * 1.0 / labeled_data_count)
    })
  }

  def confusion_matrix(valid_result: RDD[(Double, Double)]) = {
    val multiclassMetrics = new MulticlassMetrics(valid_result)
    printf("\n====>>>> overall Accuracy: %.4f\n", multiclassMetrics.accuracy)

    for (i <- multiclassMetrics.labels) {
      printf("\n====>>>> Precision: %.4f, Recall: %.4f, FMeature: %.4f\n", multiclassMetrics.precision(i), multiclassMetrics.recall(i), multiclassMetrics.fMeasure(i))
    }
  }


}
