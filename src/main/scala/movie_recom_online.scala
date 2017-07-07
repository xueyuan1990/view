package com.test

import java.text.SimpleDateFormat
import org.apache.spark.util.SizeEstimator
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
  * Created by xueyuan on 2017/6/12 完成.
  */
object movie_recom_online {
  var sc: SparkContext = null
  var hiveContext: HiveContext = null
  val sdf_date: SimpleDateFormat = new SimpleDateFormat("yyyyMMdd")
  val sdf_time: SimpleDateFormat = new SimpleDateFormat("HH:mm:ss")

  var partition_num = 1


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


    var table_out = ""
    var simp = 1.0
    var sim_movie_num = 0
    var recom_movie_num = 0
    var normalization = true
    if (args.length == 6) {//[partition_num table_out 采样率 相似矩阵中保存的相似电影数 推荐电影数 是否归一化]
      partition_num = args(0).toInt
      println("***********************partition_num=" + partition_num + "*****************************")
      table_out = args(1).toString
      println("***********************table_out=" + table_out + "*****************************")
      simp = args(2).toDouble
      println("***********************simp=" + simp + "*****************************")
      sim_movie_num = args(3).toInt
      println("***********************sim_movie_num=" + sim_movie_num + "*****************************")
      recom_movie_num = args(4).toInt
      println("***********************recom_movie_num=" + recom_movie_num + "*****************************")
      normalization = args(5).toBoolean
      println("***********************normalization=" + normalization + "*****************************")
    }

    val (user_movie_prefer, idmap, max_index) = load_movie()
    var movie_sim = item_similarity_rdd(user_movie_prefer, max_index, simp, sim_movie_num, normalization).collect()
//    for ((user, movie) <- movie_sim.take(10)) {
//      print(user + ":")
//      for ((m, sim) <- movie) {
//        print(m + "->" + sim + ", ")
//      }
//      println()
//    }

    println(sdf_time.format(new Date((System.currentTimeMillis()))) + "***********************movie_sim size = " + movie_sim.size + ",movie_sim_bytes=" + SizeEstimator.estimate(movie_sim).toDouble / (1024 * 1024 * 1024) + ",idmap_bytes=" + SizeEstimator.estimate(idmap).toDouble / (1024 * 1024 * 1024) + "*****************************")
    val movie_sim_br = sc.broadcast(movie_sim.toMap)
    val idmap_br = sc.broadcast(idmap)
    val recom_movie_num_br = sc.broadcast(recom_movie_num)

    def recom_movie(iterator: Iterator[(String, Array[(Int, Int)])]): Iterator[(String, Array[(String, Double)])] = {
      var result = new ArrayBuffer[(String, Array[(String, Double)])]() //new ArrayBuffer[(String, Array[(String, Int)], Array[(String, Double)])]()

      //相似度
      for ((user, movie_prefer_array) <- iterator) {
        var recom_movie_map = new ArrayBuffer[(String, Double)]()
        var recom_movie_set = new ArrayBuffer[Int]()
        //用户观看过的电影
        val all_watched_movie = movie_prefer_array.toMap
        //对用户偏好的电影依次寻找相似列表
        for ((movie, prefer) <- movie_prefer_array) {
          if (movie_sim_br.value.keySet.contains(movie)) {
            for ((recom_movie, sim) <- movie_sim_br.value(movie)) {
              //用户没有观看过的电影
              if (!all_watched_movie.keySet.contains(recom_movie) && !recom_movie_set.contains(recom_movie)) {
                recom_movie_set += recom_movie
              }
            }
          }
        }
        for (rm <- recom_movie_set) {
          var score = 0.0
          if (movie_sim_br.value.keySet.contains(rm)) {
            val map = movie_sim_br.value(rm).toMap
            for ((m, p) <- all_watched_movie) {
              if (map.contains(m)) {
                score += (p * map(m))
              }
            }
          }
          //转换id
          if (idmap_br.value.keySet.contains(rm)) {
            recom_movie_map += ((idmap_br.value(rm), score))
          }
        }
        var num = recom_movie_num_br.value
        if (recom_movie_map.size < num) {
          num = recom_movie_map.size
        }
        result += ((user, recom_movie_map.toArray.sortWith(_._2 > _._2).take(num))) //((user, all_watched_movie.filter(r => idmap_br.value.keySet.contains(r._1.toString)).map(r => (idmap_br.value(r._1.toString), r._2)).toArray, recom_movie_map.toArray.sortWith(_._2 > _._2)))
      }
      result.iterator
    }


    //recom movie list for each user
    user_movie_prefer.repartition(partition_num)
    val user_recommovie = user_movie_prefer.mapPartitions(recom_movie)
    save_data(table_out, user_recommovie)

  }


  def item_similarity_rdd(user_movie_prefer: RDD[(String, Array[(Int, Int)])], max_index: Int, simp: Double, sim_movie_num: Int, normalization: Boolean): RDD[(Int, Array[(Int, Double)])] = {
    val user_movie_prefer_filter_temp = user_movie_prefer.filter(r => r._2.length > 1)
    var user_movie_prefer_filter = user_movie_prefer_filter_temp
    if (simp > 0 && simp < 1) {
      user_movie_prefer_filter = user_movie_prefer_filter_temp.sample(false, simp)
    }


    //    println(sdf_time.format(new Date((System.currentTimeMillis()))) + "*********************** user_movie_prefer_filter_temp.size= " + user_movie_prefer_filter_temp.count() + ", user_movie_prefer_filter.size= " + user_movie_prefer_filter.count() + " *****************************")
    user_movie_prefer_filter.repartition(partition_num)


    val c_array = user_movie_prefer_filter.mapPartitions(iter => {
      val c = new HashMap[(Int, Int), Int]()
      for ((user, movie) <- iter) {
        for ((i, pi) <- movie; (j, pj) <- movie if !i.equals(j)) {
          val key = (i, j)
          if (c.keySet.contains(key)) {
            c.put(key, c(key) + 1)
          } else {
            c.put(key, 1)
          }
        }
      }
      c.iterator
    })

    val c_map = c_array.reduceByKey(_ + _)

    println(sdf_time.format(new Date((System.currentTimeMillis()))) + "*********************** c_map *****************************")


    val n = new Array[Int]((max_index + 1))
    for ((user, movie) <- user_movie_prefer_filter.collect()) {
      for ((i, pi) <- movie) {
        n(i) = n(i) + 1
      }
    }


    println(sdf_time.format(new Date((System.currentTimeMillis()))) + "*********************** n_length=" + n.length + " ,n_max= " + n.max + "*****************************")
    val n_br = sc.broadcast(n)
    val w_temp = c_map.mapPartitions(iter => {
      val n_value = n_br.value
      val w = new HashMap[(Int, Int), Double]
      for (((i, j), num) <- iter) {
        val temp = n_value(i) * n_value(j)
        if (temp > 0 && num > 0) {
          w.put((i, j), num.toDouble / math.sqrt(temp.toDouble))
        }
      }
      w.iterator
    })
    println(sdf_time.format(new Date((System.currentTimeMillis()))) + "*********************** w_temp *****************************")
    val w_array = w_temp.map(r => (r._1._1, Array((r._1._2, r._2)))).reduceByKey(_ ++ _)
    w_array.repartition(partition_num)
    val sim_movie_num_br = sc.broadcast(sim_movie_num)
    val normalization_br = sc.broadcast(normalization)
    val w_result = w_array.mapPartitions(iter => {
      for ((i, sim_item) <- iter) yield {
        var sim_item_sort = sim_item.sortWith(_._2 > _._2)
        if (sim_item_sort.length > sim_movie_num_br.value) {
          sim_item_sort = sim_item_sort.take(sim_movie_num_br.value)
        }
        //归一化
        if (normalization_br.value) {
          val max_i = sim_item_sort(0)._2
          var sim_item_norm = new ArrayBuffer[(Int, Double)]()
          for ((j, num) <- sim_item_sort) {
            if (max_i > 0) {
              sim_item_norm += ((j, num / max_i))
            } else {
              sim_item_norm += ((j, num))
            }

          }
          (i, sim_item_norm.toArray)
        } else {
          (i, sim_item_sort)
        }

      }

    })
    println(sdf_time.format(new Date((System.currentTimeMillis()))) + "*********************** w_norm *****************************")
    w_result

  }


  def save_data(table_out: String, user_recommovie: RDD[(String, Array[(String, Double)])]): Unit = {
    println(sdf_time.format(new Date((System.currentTimeMillis()))) + "***********************save table start*****************************")
//    for ((user, movie) <- user_recommovie.take(10)) {
//      print(user + ":")
//      for ((m, s) <- movie) {
//        print(m + "->" + s + ", ")
//      }
//      println()
//    }
    val data = user_recommovie.map(r => {
      var movie_sim_string = ""
      for ((movie, sim) <- r._2) {
        movie_sim_string += (movie + ",")
        //        movie_sim_string += (movie + ":" + sim + ", ")
      }
      val size = movie_sim_string.length
      if (size >= 2) {
        movie_sim_string = movie_sim_string.substring(0, size - 1)
      }
      (r._1, movie_sim_string)

    })
    val candidate_rdd = data.map(r => Row(r._1, r._2))

    val structType = StructType(
      StructField("source_id", StringType, false) ::
        StructField("recom_id", StringType, false) ::
        Nil
    )

    //from RDD to DataFrame
    val candidate_df = hiveContext.createDataFrame(candidate_rdd, structType)
    val create_table_sql: String = "create table if not exists " + table_out + " ( source_id  String,recom_id  String) partitioned by (stat_date bigint) stored as textfile"
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

  def load_movie(): (RDD[(String, Array[(Int, Int)])], Map[Long, String], Int) = {
    val c1 = Calendar.getInstance()
    c1.add(Calendar.DATE, -1)
    val sdf1 = new SimpleDateFormat("yyyyMMdd")
    val date1 = sdf1.format(c1.getTime())
    val sql_1 = "select imei,feature from algo.personal_recommend_usecollect_features_16 where stat_date=" + date1
    val user_movie = hiveContext.sql(sql_1).map(r => (r.getString(0), r.getString(1).split(" ")))
    //    val user_size = user_movie.count().toInt
    val sql_2 = "select column_index,code from  algo.personal_recommend_usecollect_items_16 where column_index is not null and stat_date=" + date1
    val idmap = hiveContext.sql(sql_2).map(r => (r.getLong(0), r.getString(1))).collect().toMap
    val sql_3 = "select max(column_index) from  algo.personal_recommend_usecollect_items_16 where column_index is not null and stat_date=" + date1
    val max_index = hiveContext.sql(sql_3).map(r => r.getLong(0)).collect()(0).toInt
    println(sdf_time.format(new Date((System.currentTimeMillis()))) + "***********************max_index=" + max_index + "*****************************")
    //    println(sdf_time.format(new Date((System.currentTimeMillis()))) + "*********************** load finished user size = " + user_size + "*****************************")
    val user_movie_prefer = user_movie.map(r => (r._1, r._2.map(m => (m.split(":")(0).toInt, m.split(":")(1).toInt))))
    (user_movie_prefer, idmap, max_index)
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
