package data_preprocess

import java.text.SimpleDateFormat
import java.util.Calendar

import org.apache.log4j.{Level, Logger}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, SparkSession}
import scala.collection.mutable.ArrayBuffer

/**
  * create by Wind on 2018/07/02
  */
object Prepare_Feature_app_install {

  case class Item(itemCode: String, itemName: String, itemColIndex: Long)
  case class Imei_feature(imei: String, features: String)
  case class Imei_sex(imei: String, sex: String)

  def main(args: Array[String]): Unit = {

    val sparkSession =  SparkSession.builder().enableHiveSupport().getOrCreate()

    Logger.getLogger("org").setLevel(Level.ERROR)
    Logger.getLogger("org.apache.spark").setLevel(Level.ERROR)
    Logger.getLogger("akka").setLevel(Level.ERROR)
    Logger.getRootLogger().setLevel(Level.ERROR)

    val job_time = args(0)
    // val job_time = "20180709"
    val year: Int = job_time.substring(0,4).trim.toInt
    val month: Int = job_time.substring(4,6).trim.toInt
    val day: Int = job_time.substring(6,8).trim.toInt
    val calendar: Calendar = Calendar.getInstance
    calendar.set(year,month-1,day)
    val job_date: String = new SimpleDateFormat("yyyyMMdd").format(calendar.getTime)
    printf("\n====>>>> job_date: %s\n", job_date)

    // app info
    val apps_info_table: String = "app_center.adl_sdt_adv_dim_app"
    // user action
    val users_info_table: String = "app_center.adl_fdt_app_adv_model_install"
    // user action of push
    val users_info_table_push: String = "push_service.ads_push_app_adv_model_install"

    // topK个apps
    val top_k: Int = 30000
    // table name for saving topk apps info
    val apps_table: String = "algo.yf_apps_ordered_by_install_num"
    // table name for saving user feature of app installation
    val imei_features_app_install_table: String = "algo.yf_imei_features_app_install"
    val imei_features_app_install_table_push: String = "algo.yf_imei_features_app_install_push"

    //(code_prefix+appid, appname, installnum, index))
    val save_flag = true

    printf("\n++++>>>> getting apps ordered by install num\n")
    val (top_k_apps, apps_ordered_by_install_num) = get_topK_rdd(sparkSession, top_k, apps_info_table, job_date, apps_table, save_flag)

    printf("\n++++>>>> extracting features from %s \n", users_info_table)
    val imei_features = get_user_feature_app_install(sparkSession, top_k_apps, apps_ordered_by_install_num, top_k, use_top_k_flag = false, prefix_flag = true, users_info_table, job_date, imei_features_app_install_table, save_flag)

    printf("\n++++>>>> extracting features from %s \n", users_info_table_push)
    val imei_features_push = get_user_feature_app_install(sparkSession, top_k_apps, apps_ordered_by_install_num, top_k, use_top_k_flag = false, prefix_flag = false, users_info_table_push, job_date, imei_features_app_install_table_push, save_flag)
  }

  def get_topK_rdd(sparkSession: SparkSession,
                   topK: Int,
                   data_table: String,
                   job_date: String,
                   save_table: String,
                   save_flag: Boolean): (RDD[(String, String, Long, Long)], RDD[(String, String, Long, Long)]) = {
    val select_sql: String = "select appid, app_name, installnum from " + data_table + " where stat_date = " + job_date
    printf("\n====>>>> %s\n", select_sql)

    val data = sparkSession.sql(select_sql).filter("appid is not null and app_name is not null and installnum is not null").rdd.map(v => (v(0).toString, (v(1).toString, v(2).toString.toLong))).reduceByKey((a, b) => if (a._2 >= b._2) a else b).map(v => (v._2._2, (v._1, v._2._1)))

    // ("11"app_id, app_name, install_num, index)
    val data_ordered = data.sortBy(_._1, ascending = false).zipWithIndex().map(v => (v._1._2._1, v._1._2._2, v._1._1, v._2))
    printf("\n====>>>> apps_ordered_by_install_num: %d\n", data_ordered.count())

    val top_k_apps_rdd = sparkSession.sparkContext.parallelize(data.top(topK)).zipWithIndex().map(v => (v._1._2._1, v._1._2._2, v._1._1, v._2))
    printf("\n====>>>> tok_%d apps_ordered_by_install_num: %d\n", topK, top_k_apps_rdd.count())

    if (save_flag){
      import sparkSession.implicits._
      val top_k_apps_df = data_ordered.repartition(10).toDF("app_id", "app_name", "install_num", "index")
      top_k_apps_df.createOrReplaceTempView("temp")
      val create_sql = "create table if not exists " + save_table + " (app_id string, app_name string, install_num bigint, index bigint) partitioned by (stat_date bigint) ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.columnar.ColumnarSerDe' STORED AS RCFILE"
      val insert_sql: String = "insert overwrite table " + save_table + " partition(stat_date = " + job_date + ") select * from temp"
      printf("\n====>>>> %s\n====>>>> %s\n", create_sql, insert_sql)
      sparkSession.sql(create_sql)
      sparkSession.sql(insert_sql)
    }
    (top_k_apps_rdd, data_ordered)
  }

  def get_user_feature_app_install(sparkSession: SparkSession,
                                   top_k_apps: RDD[(String, String, Long, Long)],
                                   apps_ordered_by_install_num: RDD[(String, String, Long, Long)],
                                   top_k: Int,
                                   use_top_k_flag: Boolean,
                                   prefix_flag: Boolean,
                                   data_table: String,
                                   job_date: String,
                                   save_table: String,
                                   save_flag: Boolean) = {
    var apps_map: collection.Map[String, (String, Long)] = null
    if (use_top_k_flag) {
      printf("\n====>>>> use the top_%d apps to generate features", top_k)
      apps_map = top_k_apps.map(v => (v._1, (v._2, v._4))).collectAsMap()
    } else {
      printf("\n====>>>> use all of apps (%d) to generate features", apps_ordered_by_install_num.count())
      apps_map = apps_ordered_by_install_num.map(v => (v._1, (v._2, v._4))).collectAsMap()
    }

    if (prefix_flag) {
      printf("\n====>>>> using prefix: %s\n", "11")
      val temp = apps_map.map(v => ("11"+v._1, v._2))
      apps_map = temp
    }

    val select_sql: String = "select imei, value from " + data_table + " where stat_date=" + job_date
    printf("\n====>>>> %s\n", select_sql)

    val data = sparkSession.sql(select_sql).filter("imei is not null and value is not null").rdd.map(v => {
      val imei: String = v(0).toString
      val app_install_str = v(1).toString.split(" ")
      val app_install_array = ArrayBuffer[((String, String), Int)]()
      var imei_app_install_feat_array = ArrayBuffer[String]()
      for (item <- app_install_str) {
        val app_id: String = item.split(":")(0)
        if (apps_map.contains(app_id)) {
          val app_name = apps_map(app_id)._1
          val index = apps_map(app_id)._2.toString.toInt
          app_install_array += (((app_id, app_name), index))
        }
      }
      if (app_install_array.nonEmpty)
        (imei, app_install_array.sortBy(_._2))
      else
        (imei, app_install_array)
    })

    printf("\n====>>>> imei count(total): %d\n", data.count())

    val data_invalid = data.filter(_._2.isEmpty)
    printf("\n====>>>> imei count(app install list is empty): %d\n", data_invalid.count())

    val data_refined = data.filter(_._2.nonEmpty)
    printf("\n====>>>> imei count(refined by excluding empty list of app install): %d\n", data_refined.count())

    val data_refined_ordered = data_refined.map(v => (v._2.length, (v._1, v._2)))
    val min_len = data_refined_ordered.map(_._1).min()
    val max_len = data_refined_ordered.map(_._1).max()
    printf("\n====>>>> max/min app install num of imei: %d, %d\n", min_len, max_len)

    val imei_features = data_refined.map(v => {
      val imei = v._1

      val feat = v._2.map(v => "[" + v._1._1 + "_" + v._1._2 + "]_wind_" + v._2.toString + ":1").mkString(" ")
      (imei, feat)
    })

    if (save_flag){
      import sparkSession.implicits._
      val top_k_apps_df = imei_features.repartition(10).toDF("imei", "feature")
      top_k_apps_df.createOrReplaceTempView("temp")
      val create_sql = "create table if not exists " + save_table + " (imei string, feature string) partitioned by (stat_date bigint) ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.columnar.ColumnarSerDe' STORED AS RCFILE"
      val insert_sql: String = "insert overwrite table " + save_table + " partition(stat_date = " + job_date + ") select * from temp"
      printf("\n====>>>> %s\n====>>>> %s\n", create_sql, insert_sql)
      sparkSession.sql(create_sql)
      sparkSession.sql(insert_sql)
    }
    imei_features
  }

//  def get_user_feature_app_install_push(sparkSession: SparkSession,
//                                   top_k_apps: RDD[(String, String, Long, Long)],
//                                   top_k: Int,
//                                   data_table: String,
//                                   job_date: String,
//                                   save_table: String,
//                                   save_flag: Boolean) = {
//    val select_sql: String = "select imei, value from " + data_table + " where stat_date=" + job_date
//    printf("\n====>>>> %s\n", select_sql)
//
//    val top_k_apps_map = top_k_apps.map(v => (v._1, v._4)).collectAsMap()
//
//    val data = sparkSession.sql(select_sql).filter("imei is not null and value is not null").rdd.map(v => {
//      val imei: String = v(0).toString
//      val app_install_str = v(1).toString.split(" ")
//      val app_install_array = ArrayBuffer[(String, Int)]()
//      var imei_app_install_feat_array = ArrayBuffer[String]()
//      for (item <- app_install_str) {
//        val app_id: String = "11" + item.split(":")(0)
//        if (top_k_apps_map.contains(app_id)) {
//          val index = top_k_apps_map(app_id).toString.toInt
//          app_install_array += ((app_id, index))
//        }
//        // app_install_array += ((app_id, 1))
//      }
//      if (app_install_array.nonEmpty)
//        (imei, app_install_array.sortBy(_._2))
//      else
//        (imei, app_install_array)
//    })
//    printf("\n====>>>> imei count(total): %d\n", data.count())
//
//    val data_invalid = data.filter(_._2.isEmpty)
//    printf("\n====>>>> imei count(app install list is empty): %d\n", data_invalid.count())
//
//    val data_refined = data.filter(_._2.nonEmpty)
//    printf("\n====>>>> imei count(refined by excluding empty list of app install): %d\n", data_refined.count())
//
//    val data_refined_ordered = data_refined.map(v => (v._2.length, (v._1, v._2)))
//    val min_len = data_refined_ordered.map(_._1).min()
//    val max_len = data_refined_ordered.map(_._1).max()
//    printf("\n====>>>> max/min app install num of imei: %d, %d\n", min_len, max_len)
//
//    val imei_features = data_refined.map(v => {
//      val imei = v._1
//      val feat = (v._2.map(_._2).mkString(":1 ") + ":1").trim
//      (imei, feat)
//    })
//
//    if (save_flag){
//      import sparkSession.implicits._
//      val top_k_apps_df = imei_features.repartition(10).toDF("imei", "feature")
//      top_k_apps_df.createOrReplaceTempView("temp")
//      val create_sql = "create table if not exists " + save_table + " (imei string, feature string) partitioned by (stat_date bigint) ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.columnar.ColumnarSerDe' STORED AS RCFILE"
//      val insert_sql: String = "insert overwrite table " + save_table + " partition(stat_date = " + job_date + ") select * from temp"
//      printf("\n====>>>> %s\n%s\n", create_sql, insert_sql)
//      sparkSession.sql(create_sql)
//      sparkSession.sql(insert_sql)
//    }
//  }
}

