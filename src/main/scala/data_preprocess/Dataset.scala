package data_preprocess

import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, SparkSession}

import scala.collection.mutable.ArrayBuffer

import utils.My_Utils


object Dataset {
  def get_gender_labeled_data(sparkSession: SparkSession, job_date: String): RDD[(String, Double)] = {
    val select_gender_label_sql: String = "select imei, sex from " + "(select user_id,sex from user_center.mdl_flyme_users_info where stat_date= " + job_date + " and (sex= 2 OR sex=1)) t " + "join (select imei,uid from  user_profile.edl_device_uid_mz_rel where stat_date= " + job_date + " ) s on (t.user_id = s.uid)"
    printf("\n====>>>> %s\n", select_gender_label_sql)
    val gender_label_df: DataFrame = sparkSession.sql(select_gender_label_sql).filter("imei is not null and sex is not null")

    val gender_label_rdd = gender_label_df.rdd.map(v => {
      var sex: Double = -1
      if (v(1) == 2)
        sex = 1 // M
      else
        sex = 0 // F
      (v(0).toString, sex)
    }).filter(_._2 != -1).reduceByKey((x, y) => x)

    My_Utils.numerical_label_distribute(gender_label_rdd)

    gender_label_rdd
  }

  def get_age_labeled_data(sparkSession: SparkSession, job_date: String): RDD[(String, Double)] = {
    val flyme_account_table: String = "user_center.mdl_flyme_users_info"
    val flyme_imei_id_table: String = "user_profile.dwd_user_uid_basic_ext"

    val select_imei_birthday_sql = "select b.imei, a.birthdate from (select user_id,birthdate from " + flyme_account_table +" where stat_date = " + job_date + " and length(birthdate) > 0) a join (select imei, uid from " + flyme_imei_id_table +" where stat_date = " + job_date + ") b on a.user_id = b.uid"
    printf("\n====>>>> %s\n", select_imei_birthday_sql)

    val dataset = sparkSession.sql(select_imei_birthday_sql).filter("imei is not null and birthdate is not null")

    val dataset_label: RDD[(String,Double)] = dataset.rdd.map(r => {
      var label: Int = -1
      val imei = r(0).toString.trim
      val birthday = r(1).toString.trim.substring(0,4).toInt

      val year = job_date.substring(0,4).toInt //current year
      val age = year - birthday
      if (age >= 7&& age <= 14)
        label = 0
      else if (age >= 15&& age <= 22)
        label = 1
      else if (age >= 23&& age <= 35)
        label = 2
      else if (age >= 36&& age <= 45)
        label = 3
      else if (age >= 46&& age <= 76)
        label = 4
      (imei,label.toDouble)
    }).filter(_._2 != -1)

    printf("\n====>>>> dataset label: %d\n", dataset_label.count())
    My_Utils.numerical_label_distribute(dataset_label)
    dataset_label
  }

  def get_marriage_label_from_flyme_age(sparkSession: SparkSession, job_date: String): RDD[(String, Double)] = {
    val user_age_from_flyme_table_name: String = "algo.yf_user_age_collect_from_flyme"
    val select_user_age_sql: String = "select * from " + user_age_from_flyme_table_name + " where stat_date=" + job_date
    printf("\n====>>>> %s\n", select_user_age_sql)

    val user_marriage_label = sparkSession.sql(select_user_age_sql).rdd.map(v => {
      val imei: String = v(0).toString
      val age: Int = v(2).toString.toInt
      var label: Double = -1
      if (age <= 35)
        label = 0
      if (age > 35)
        label = 1
      (imei, label)
    }).filter(_._2 != -1).reduceByKey((a, b) => a)

    My_Utils.numerical_label_distribute(user_marriage_label)

    user_marriage_label
  }

  def get_marriage_label_from_questionnaire(sparkSession: SparkSession, questionnaire_table_name: String): RDD[(String, Double)] = {
    val select_sql: String = "SELECT imei, q4 from " + questionnaire_table_name + " where q4==\"A\" OR q4==\"B\" OR q4==\"C\" OR q4==\"D\""
    printf("\n====>>>> %s\n", select_sql)

    val user_marriage_label_raw = sparkSession.sql(select_sql).filter("imei is not null and q4 is not null").rdd.map(v => (v(0).toString, v(1).toString))

    val user_marriage_label = user_marriage_label_raw.map(v => {
      var label: Double = -1
      if (v._2 == "A")
        label = 0
      if (v._2 == "B")
        label = 1
      (v._1, label)
    }).filter(_._2 != -1).reduceByKey((a, b) => a)

    My_Utils.numerical_label_distribute(user_marriage_label)

    user_marriage_label
  }

  def get_parent_label_from_questionnaire(sparkSession: SparkSession, questionnaire_table_name: String): RDD[(String, Double)] = {
    val questionnaire_select_sql: String = "SELECT imei, Q7 from " + questionnaire_table_name + " where Q7!=\"\""
    printf("\n====>>>> %s\n", questionnaire_select_sql)

    val parent_labeled_data = sparkSession.sql(questionnaire_select_sql).filter("imei is not null and Q7 is not null").rdd.filter(v => v(1).toString.length == 1).map(v => (v(0).toString, v(1).toString))
    printf("\n====>>>> parent labeled data: %\n", parent_labeled_data.count)

    val parent_labeled_data_refined = parent_labeled_data.map(v => {
      var label: Double = -1
      if(v._2 == "A")
        label = 1
      else if(v._2 == "B")
        label = 1
      else if(v._2 == "C")
        label = 1
      else if(v._2 == "D")
        label = 1
      else if(v._2 == "E")
        label = 1
      else if(v._2 == "F")
        label = 1
      else if(v._2 == "H")
        label = 0
      else label = 2
      (v._1, label)
    }).filter(_._2 != -1).filter(_._2 != 2).reduceByKey((a, b) => a)

    My_Utils.numerical_label_distribute(parent_labeled_data_refined)

    parent_labeled_data_refined
  }

  def get_child_label_from_xxx_data(sparkSession: SparkSession, xxx_table_name: String): RDD[(String, Double)] = {
    val xxx_child_stage_sql: String = "select imei, child_stage from " + xxx_table_name + " where child_stage=\"婴幼儿\" or child_stage=\"孕育期\" or child_stage=\"青少年\""
    printf("\n====>>>> %s\n", xxx_child_stage_sql)

    val xxx_child_stage_labeled_data = sparkSession.sql(xxx_child_stage_sql).filter("imei is not null and child_stage is not null").rdd.map(v => {
      var label: Double = -1
      if(v(1).toString == "婴幼儿")
        label = 0
      else if(v(1).toString == "孕育期")
        label = 1
      else if(v(1).toString == "青少年")
        label = 2
      else label = 3
      (v(0).toString, label)
    }).filter(_._2 != -1).filter(_._2 != 3).reduceByKey((a, b) => a)

    My_Utils.numerical_label_distribute(xxx_child_stage_labeled_data)

    xxx_child_stage_labeled_data
  }

  def get_carowner_label_from_questionnaire(sparkSession: SparkSession, questionnaire_table_name: String, yestoday_Date: String): RDD[(String, Double)] = {
    val select_sql: String = "select imei, q8 from " + questionnaire_table_name + " where Q8!=\"\""
    printf("\n====>>>> %s\n", select_sql)

    val carowner_labeled_data = sparkSession.sql(select_sql).filter("imei is not null and q8 is not null").rdd.map(v => (v(0).toString, v(1).toString)).map(v => {
      var label: Double = -1
      if(v._2 == "A") //有车
        label = 1
      else if(v._2 == "B") //没车
        label = 0
      else label = 2
      (v._1, label)
    }).filter(_._2 != -1).filter(_._2 != 2).reduceByKey((a, b) => a)

    My_Utils.numerical_label_distribute(carowner_labeled_data)
    carowner_labeled_data
  }

  def get_consumption_label_from_questionnaire(sparkSession: SparkSession, questionnaire_table_name: String): RDD[(String, Double)] = {
    val select_sql: String = "select imei, q11 from " + questionnaire_table_name + " where Q11!=\"\""
    printf("\n====>>>> %s\n", select_sql)

    val consumption_labeled_data = sparkSession.sql(select_sql).filter("imei is not null and q11 is not null").rdd.map(v => (v(0).toString, v(1).toString)).map(v => {
      var label: Double = -1
      if(v._2 == "A")//5000以下
        label = 0
      else //5000以上
        label = 1
      (v._1, label)
    }).filter(_._2 != -1).reduceByKey((a, b) => a)

    My_Utils.numerical_label_distribute(consumption_labeled_data)
    consumption_labeled_data
  }

  def get_estate_label_from_questionnaire(sparkSession: SparkSession, questionnaire_table_name: String): RDD[(String, Double)] = {
    val select_sql: String = "select imei, q9 from " + questionnaire_table_name + " where Q9!=\"\""
    printf("\n====>>>> %s\n", select_sql)

    val estate_labeled_data = sparkSession.sql(select_sql).filter("imei is not null and q9 is not null").rdd.map(v => (v(0).toString, v(1).toString)).map(v => {
      var label: Double = -1
      if(v._2 == "A")//有房
        label = 1
      else if(v._2 == "B")//没房
        label = 0
      else label = 2
      (v._1, label)
    }).filter(_._2 != -1).filter(_._2 != 2).reduceByKey((a, b) => a)

    My_Utils.numerical_label_distribute(estate_labeled_data)
    estate_labeled_data
  }

  def get_income_label_from_questionnaire(sparkSession: SparkSession, questionnaire_table_name: String): RDD[(String, Double)] = {
    val select_sql: String = "select imei, q10 from " + questionnaire_table_name + " where Q10!=\"\""
    printf("\n====>>>> %s\n", select_sql)

    val income_labeled_data = sparkSession.sql(select_sql).filter("imei is not null and q10 is not null").rdd.map(v => (v(0).toString, v(1).toString)).map(v => {
      var label: Double = -1
      if(v._2 == "A")//5000以下
        label = 0
      else //5000以上
        label = 1
      (v._1, label)
    }).filter(_._2 != -1).reduceByKey((a, b) => a)

    My_Utils.numerical_label_distribute(income_labeled_data)
    income_labeled_data
  }

  def get_education_label_from_questionnarie(sparkSession: SparkSession, questionnaire_table_name: String): RDD[(String, Double)] = {
    val select_sql: String = "select imei, q3 from " + questionnaire_table_name + " where Q3!=\"\""
    printf("\n====>>>> %s\n", select_sql)

    val education_labeled_data = sparkSession.sql(select_sql).filter("imei is not null and q3 is not null").rdd.map(v => (v(0).toString, v(1).toString)).map(v => {
      var label: Double = -1
      if(v._2 == "A")
        label = 0
      else if(v._2 == "B")
        label = 0
      else if(v._2 == "C")
        label = 0
      else if(v._2 == "D")
        label = 0
      else if(v._2 == "E")
        label = 1
      else if(v._2 == "F")
        label = 2
      else if(v._2 == "G")
        label = 2
      else label = 3
      (v._1, label)
    }).filter(_._2 != -1).filter(_._2 != 3).reduceByKey((a, b) => a)

    My_Utils.numerical_label_distribute(education_labeled_data)
    education_labeled_data
  }

  def get_career_label_from_questionnaire(sparkSession: SparkSession, questionnaire_table_name: String): RDD[(String, Double)] = {
    val select_sql: String = "select imei, q6 from " + questionnaire_table_name + " where Q6!=\"\""
    printf("\n====>>>> %s\n", select_sql)

    val career_labeled_data = sparkSession.sql(select_sql).filter("imei is not null and q6 is not null").rdd.map(v => (v(0).toString, v(1).toString)).map(v => {
      var label: Double = -1
      if(v._2 == "A") //学生
        label = 0
      else if(v._2 == "B")//工人
        label = 1
      else if(v._2 == "C")//服务行业
        label = 1
      else if(v._2 == "I")//IT
        label = 1
      else if(v._2 == "F")//个人店主
        label = 1
      else if(v._2 == "Q")//建筑
        label = 1
      else if(v._2 == "D")//公务人员
        label = 1
      else if(v._2 == "E")//教育
        label = 1
      //      else if(charToInt.equals("L"))//农业
      //        label = "6"
      else if(v._2 == "G")//医药
        label = 1
      else if(v._2 == "J")//金融
        label = 1
      (v._1, label)
    }).filter(_._2 != -1).reduceByKey((a, b) => a)

    My_Utils.numerical_label_distribute(career_labeled_data)
    career_labeled_data
  }


  def get_dataset(sparkSession: SparkSession, labeled_dataset: RDD[(String, Double)], imei_features_table: String, feature_dim: Int, job_date: String): (RDD[(String, LabeledPoint)], RDD[(String, LabeledPoint)]) = {

    val select_features_sql: String = "select imei, feature from " + imei_features_table + " where stat_date=" + job_date
    printf("\n====>>>> %s\n", select_features_sql)
    val imei_features_df = sparkSession.sql(select_features_sql).filter("imei is not null and feature is not null")

    val dataset: RDD[(String, String, Double)] = imei_features_df.rdd.map(v => (v(0).toString, v(1).toString)).leftOuterJoin(labeled_dataset).map(v => (v._1, v._2._1, v._2._2.getOrElse(-1)))

    val dataset_label_point = dataset.map(v => {
      val imei = v._1
      val features_array = v._2.split(" ")
      val index_array = new ArrayBuffer[Int]()
      val value_array = new ArrayBuffer[Double]()

      for (item <- features_array){
        val index_value = item.trim.split(":")
        if (index_value.length == 2){
          val tmp = index_value(0).trim.split("_wind_")
          if (tmp.length == 2) {
            val index = tmp(1).toInt
            if (index <= feature_dim-1) {
              index_array += index
              value_array += index_value(1).trim.toDouble
            }
          }
        }
      }
      (imei, index_array, value_array, v._3)
    }).filter(v => v._2.nonEmpty && v._3.nonEmpty).map(v => (v._1, new LabeledPoint(v._4, Vectors.sparse(feature_dim, v._2.toArray, v._3.toArray))))

    My_Utils.numerical_label_distribute(dataset_label_point.map(v => (v._1, v._2.label)))

    val dataset_labeled = dataset_label_point.filter(_._2.label != -1)
    val dataset_unlabeled = dataset_label_point.filter(_._2.label == -1)
    printf("\n====>>>> dataset_labeled: %d\n", dataset_labeled.count())
    printf("\n====>>>> dataset_unlabeled: %d\n", dataset_unlabeled.count())

    (dataset_labeled, dataset_unlabeled)
  }

}
