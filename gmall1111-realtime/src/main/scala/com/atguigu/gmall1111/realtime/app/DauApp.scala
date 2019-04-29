package com.atguigu.gmall1111.realtime.app

import java.text.SimpleDateFormat
import java.util
import java.util.Date

import com.alibaba.fastjson.JSON
import com.atguigu.gmall1111.common.constant.GmallConstant
import com.atguigu.gmall1111.common.util.MyEsUtil
import com.atguigu.gmall1111.realtime.bean.StartUpLog
import com.atguigu.gmall1111.realtime.util.{MyKafkaUtil, RedisUtil}
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import redis.clients.jedis.Jedis

object DauApp {

  def main(args: Array[String]): Unit = {

      val sparkConf: SparkConf = new SparkConf().setAppName("dau_app").setMaster("local[*]")

      val ssc = new StreamingContext(new SparkContext(sparkConf),Seconds(5))

      val inputDStream: InputDStream[ConsumerRecord[String, String]] = MyKafkaUtil.getKafkaStream(GmallConstant.KAFKA_TOPIC_STARTUP,ssc)

//      inputDStream.foreachRDD{ rdd=>
//        println(rdd.map(_.value()).collect().mkString("\n"))
//
//      }

      //

      //  1 把当日已访问过的用户保存起来 redis
      //  2  以当日已访问用户清单为依据 ，过滤掉再次访问的请求



    // 转换case class  补全日期格式
    val startupLogDstream: DStream[StartUpLog] = inputDStream.map { record =>
      val json: String = record.value()
      val startUpLog: StartUpLog = JSON.parseObject(json, classOf[StartUpLog])
      //把日期进行补全
      val datetimeString: String = new SimpleDateFormat("yyyy-MM-dd HH:mm").format(new Date(startUpLog.ts))
      val datetimeArray: Array[String] = datetimeString.split(" ")
      startUpLog.logDate = datetimeArray(0)
      startUpLog.logHour = datetimeArray(1).split(":")(0)
      startUpLog.logHourMinute = datetimeArray(1)
      startUpLog
    }

    // 去重操作
    val filteredDstream: DStream[StartUpLog] = startupLogDstream.transform { rdd =>
      //driver 每时间间隔执行一次
      println("过滤前："+  rdd.count())
      val jedis: Jedis = RedisUtil.getJedisClient
      val curDate: String = new SimpleDateFormat("yyyy-MM-dd").format(new Date())
      val key: String = "dau:" + curDate
      val dauSet: util.Set[String] = jedis.smembers(key) //得到当日日活用户清单
      val dauBC: Broadcast[util.Set[String]] = ssc.sparkContext.broadcast(dauSet)

      val filteredRDD: RDD[StartUpLog] = rdd.filter { startuplog => //executor
        !dauBC.value.contains(startuplog.mid)
      }
      println("过滤后："+  filteredRDD.count())
      filteredRDD
    }
    // 考虑到 新的访问可能会出现重复 ，所以以mid为key进行去重，每个mid为小组 每组取其中一个
    val startuplogGroupbyMid: DStream[(String, Iterable[StartUpLog])] = filteredDstream.map{startuplog=>(startuplog.mid,startuplog)}.groupByKey()
    val startuplogFilterDistinctDstream: DStream[StartUpLog] = startuplogGroupbyMid.flatMap { case (mid, startuplogItr) =>
      val startupLogOnceItr: Iterable[StartUpLog] = startuplogItr.take(1)
      startupLogOnceItr
    }

//
//    val jedis: Jedis = RedisUtil.getJedisClient
//    val key: String = "dau:"+startupLog.logDate
//    jedis.smembers(key)

//    startupLogDstream.filter{ startupLog=>    // 由于频繁访问redis 建议优化
//      val jedis: Jedis = RedisUtil.getJedisClient
//      val key: String = "dau:"+startupLog.logDate
//       !jedis.sismember(key,startupLog.mid)
//    }



    //  1 把当日已访问过的用户保存起来 redis
    startuplogFilterDistinctDstream.foreachRDD{rdd=>
       //driver
      rdd.foreachPartition {startupLogItr=>   // executor
        //   设计保存的key  类型 set   key: dau:2019-xx-xx  value: mid
        //  sadd key value
        val jedis: Jedis = RedisUtil.getJedisClient //executor
        val startupLogList: List[StartUpLog] = startupLogItr.toList
        for ( startupLog<- startupLogList ) {
          val key: String = "dau:"+startupLog.logDate
          jedis.sadd(key,startupLog.mid)
           //保存到es

        }
        jedis.close()

        MyEsUtil.insertEsBulk(GmallConstant.ES_INDEX_DAU,startupLogList)

      }

    }
    ssc.start()
    ssc.awaitTermination()


  }

}
