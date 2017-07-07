package com.test

import java.text.SimpleDateFormat
import java.util
import java.util.{Calendar, Date}

import org.apache.log4j.{Level, Logger}
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.{Row, SQLContext}
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.sql.types.{LongType, StringType, StructField, StructType}

import scala.collection.mutable.HashMap
import scala.collection.mutable.ArrayBuffer

/**
  * Created by xueyuan on 2017/6/5.
  */
object movie_recom2 {
  var sc: SparkContext = null
  var hiveContext: HiveContext = null
  val sdf_date: SimpleDateFormat = new SimpleDateFormat("yyyyMMdd")
  val sdf_time: SimpleDateFormat = new SimpleDateFormat("HH:mm:ss")
  var simp = 1.0
  def main(args: Array[String]): Unit = {
    val userName = "mzsip"
    System.setProperty("user.name", userName)
    System.setProperty("HADOOP_USER_NAME", userName)
    println("***********************start*****************************")
    val sparkConf: SparkConf = new SparkConf().setAppName("vedio_training_xueyuan")
    sc = new SparkContext(sparkConf)
    val sqlContext = new SQLContext(sc)
    println("***********************sc*****************************")
    sc.hadoopConfiguration.set("mapred.output.compress", "false")
    hiveContext = new HiveContext(sc)
    println("***********************hive*****************************")
    Logger.getLogger("org.apache.spark").setLevel(Level.WARN)

    var partition_num = 1
    var table_out = ""
    if (args.length == 2) {
      simp = args(0).toDouble
      println("***********************simp=" + simp + "*****************************")
      partition_num = args(1).toInt
      println("***********************partition_num=" + partition_num + "*****************************")
      table_out = args(2).toString
      println("***********************table_out=" + table_out + "*****************************")
    }


    val (movie_vector, user_movie_prefer, idmap) = load_movie()
    println(sdf_time.format(new Date((System.currentTimeMillis()))) + "***********************get movie vector*****************************")
    val movie_vector_br = sc.broadcast(movie_vector.collect())

    //sim
    def find_sim(iterator: Iterator[(String, Array[Int])]): Iterator[(String, Array[(String, Double)])] = {
      var result: ArrayBuffer[(String, Array[(String, Double)])] = new ArrayBuffer[(String, Array[(String, Double)])]()
      val movie_vector_total = movie_vector_br.value
      while (iterator.hasNext) {
        var sim_array: ArrayBuffer[(String, Double)] = new ArrayBuffer[(String, Double)]()
        val r = iterator.next()
        for ((m, v) <- movie_vector_total if !m.equals(r._1)) {
          sim_array += ((m, sim(r._2, v)))
        }
        val sim_apps = sim_array.sortWith(_._2 > _._2).take(10).toArray
        result += ((r._1, sim_apps))
      }
      result.iterator
    }

    movie_vector.repartition(partition_num)
    val movie_sim = movie_vector.mapPartitions(find_sim).collect().toMap
    println(sdf_time.format(new Date((System.currentTimeMillis()))) + "***********************get movie sim*****************************")
    val movie_sim_br = sc.broadcast(movie_sim)
    val idmap_br = sc.broadcast(idmap)

    def recom_movie(iterator: Iterator[(String, Array[(String, Int)])]): Iterator[(String, Array[(String, Double)])] = {
      var result = new ArrayBuffer[(String, Array[(String, Double)])]()


      //相似度
      val movie_sim_array = movie_sim_br.value
      for ((user, movie_prefer_array) <- iterator) {
        var recom_movie_map = new ArrayBuffer[(String, Double)]()
        var recom_movie_set = new ArrayBuffer[String]()
        //用户观看过的电影
        val all_watched_movie = movie_prefer_array.toMap
        //对用户偏好的电影依次寻找相似列表
        for ((movie, prefer) <- movie_prefer_array) {
          for ((recom_movie, sim) <- movie_sim_array(movie)) {
            //用户没有观看过的电影
            if (!all_watched_movie.keySet.contains(recom_movie) && !recom_movie_set.contains(recom_movie)) {
              recom_movie_set += recom_movie
            }
          }
        }
        //对用户没有观看过的电影打分
        for (rm <- recom_movie_set) {
          var score = 0.0
          for ((m, p) <- all_watched_movie) {
            for ((rm2, sim) <- movie_sim_array(m)) {
              if (rm.equals(rm2)) {
                score += (p * sim)
              }
            }
          }
          //转换id
          recom_movie_map += ((idmap_br.value(rm), score))
        }
        if (recom_movie_map.size > 10) {
          recom_movie_map = recom_movie_map.sortWith(_._2 > _._2).take(10)
        }
        result += ((user, recom_movie_map.toArray.sortWith(_._2 > _._2)))
      }
      result.iterator
    }


    //recom movie list for each user
    user_movie_prefer.repartition(partition_num)
    val user_recommovie = user_movie_prefer.mapPartitions(recom_movie)
    println(sdf_time.format(new Date((System.currentTimeMillis()))) + "***********************get recom movie*****************************")
    save_data(table_out, user_recommovie)

  }

  def save_data(table_out: String, user_recommovie: RDD[(String, Array[(String, Double)])]): Unit = {
    println(sdf_time.format(new Date((System.currentTimeMillis()))) + "***********************save table start*****************************")
    val data = user_recommovie.map(r => {
      var app_sim_string = ""
      for ((app, sim) <- r._2) {
        app_sim_string += (app + ":" + sim + ", ")
      }
      (r._1, app_sim_string)

    })
    val candidate_rdd = data.map(r => Row(r._1, r._2))

    val structType = StructType(
      StructField("item", StringType, false) ::
        StructField("sim_items", StringType, false) ::
        Nil
    )

    //from RDD to DataFrame
    val candidate_df = hiveContext.createDataFrame(candidate_rdd, structType)
    val create_table_sql: String = "create table if not exists " + table_out + " (id BIGINT, app_package String, sim_apps String) partitioned by (stat_date bigint) stored as textfile"
    val c1 = Calendar.getInstance()
    c1.add(Calendar.DATE, -1)
    val sdf1 = new SimpleDateFormat("yyyyMMdd")
    val date1 = sdf1.format(c1.getTime())
    val insertInto_table_sql: String = "insert overwrite table " + table_out + " partition(stat_date = " + date1 + ") select * from "
    val table_temp = "movie_temp"
    println(sdf_time.format(new Date((System.currentTimeMillis()))) + "***********************save data start*****************************")
    candidate_df.registerTempTable(table_temp)
    println(sdf_time.format(new Date((System.currentTimeMillis()))) + "***********************register TempTable finished*****************************")
    hiveContext.sql(create_table_sql)
    println(sdf_time.format(new Date((System.currentTimeMillis()))) + "***********************create table finished*****************************")
    hiveContext.sql(insertInto_table_sql + table_temp)
    println(sdf_time.format(new Date((System.currentTimeMillis()))) + "***********************insertInto table finished*****************************")
  }

  def load_movie(): (RDD[(String, Array[Int])], RDD[(String, Array[(String, Int)])], Map[String, String]) = {
    val c1 = Calendar.getInstance()
    c1.add(Calendar.DATE, -1)
    val sdf1 = new SimpleDateFormat("yyyyMMdd")
    val date1 = sdf1.format(c1.getTime())
    val sql_1 = "select imei,feature from algo.personal_recommend_usecollect_features_16 where stat_date=" + date1
    val user_movie = hiveContext.sql(sql_1).map(r => (r.getString(0), r.getString(1).split(" ")))
    val user_size = user_movie.count().toInt
    val sql_2 = "select column_index,code from  algo.personal_recommend_usecollect_items_16 where column_index is not null and stat_date=" + date1
    val idmap = hiveContext.sql(sql_2).map(r => (r.getLong(0).toString, r.getString(1))).collect().toMap
    println(sdf_date.format(new Date((System.currentTimeMillis()))) + "*********************** load finished user size = " + user_size + "*****************************")


    val user_movie_prefer = user_movie.map(r => (r._1, r._2.map(m => (m.split(":")(0), m.split(":")(1).toInt))))
    val user_movie_userid = user_movie.map(r => (r._1, r._2.map(m => (m.split(":")(0))))).filter(r => r._2.length > 1).zipWithUniqueId() //filter user who watch movie less than 1

    val user_size_br = sc.broadcast((user_size*simp).toInt)
    val movie_vector = user_movie_userid.map(r => {
      val m_vec = new HashMap[String, Array[Int]]()
      val userid = r._2.toInt
      for (m <- r._1._2) {
        val keys = m_vec.keySet
        if (keys.contains(m)) {
          val vec = m_vec(m)
          vec(userid) = 1
        } else {
          val vec = new Array[Int](user_size_br.value)
          if(userid<vec.length){
            vec(userid) = 1
            m_vec.put(m, vec)
          }

        }
      }
      m_vec.toArray
    }).flatMap(r => r)
    (movie_vector, user_movie_prefer, idmap)
  }

  def sim(word1: Array[Int], word2: Array[Int]): Double = {
    val member = word1.zip(word2).map(d => d._1 * d._2).reduce(_ + _)
    //求出分母第一个变量值
    val temp1 = math.sqrt(word1.map(num => {
      math.pow(num, 2)
    }).reduce(_ + _))
    //求出分母第二个变量值
    val temp2 = math.sqrt(word2.map(num => {
      math.pow(num, 2)
    }).reduce(_ + _))
    //求出分母
    val denominator = temp1 * temp2
    val sim = member / denominator
    sim
  }
}
