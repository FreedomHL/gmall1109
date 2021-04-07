package com.atguigu.app

import java.text.SimpleDateFormat
import java.util
import java.util.Date

import com.alibaba.fastjson.JSON
import com.atguigu.GmallConstants
import com.atguigu.bean.{CouponAlertInfo, EventLog}
import com.atguigu.util.MyKafkaUtil
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.{Minutes, Seconds, StreamingContext}

import scala.collection.mutable
import scala.util.control.Breaks._

object AlertTeacher {
  def main(args: Array[String]): Unit = {
    //1.创建SparkConf
    val sparkConf: SparkConf = new SparkConf().setAppName("AlertApp").setMaster("local[*]")

    //2.创键StreamingContext
    val ssc: StreamingContext = new StreamingContext(sparkConf, Seconds(5))

    //3.连接kafka消费kafka数据
    val kafkaDStream: InputDStream[ConsumerRecord[String, String]] = MyKafkaUtil.getKafkaStream(GmallConstants.KAFKA_TOPIC_EVENT, ssc)

    //    kafkaDStream.foreachRDD(rdd=>{
    //      rdd.foreach(log=>{
    //        println(log.value())
    //      })
    //    })

    //4.将json转换为样例类并将数据转换为KV(mid,log)
    val sdf: SimpleDateFormat = new SimpleDateFormat("yyyy-MM-dd HH")
    val midToLogDStream: DStream[(String, EventLog)] = kafkaDStream.mapPartitions(partition => {
      partition.map(record => {
        val eventLog: EventLog = JSON.parseObject(record.value(), classOf[EventLog])
        //补全logdate and  loghour
        val time: String = sdf.format(new Date(eventLog.ts))
        eventLog.logDate = time.split(" ")(0)
        eventLog.logHour = time.split(" ")(1)
        (eventLog.mid, eventLog)
      })
    })

    //5.开窗
    val windowMidToLogDStream: DStream[(String, EventLog)] = midToLogDStream.window(Minutes(5))

    //6.将相同mid的数据聚和到一起
    val midToLogIterDStream: DStream[(String, Iterable[EventLog])] = windowMidToLogDStream.groupByKey()

    //7.根据条件过滤数据
    val boolToCouponAlertDStream: DStream[(Boolean, CouponAlertInfo)] = midToLogIterDStream.mapPartitions(partiton => {
      partiton.map { case (mid, iter) => {
        //        val uids: mutable.HashSet[String] = new mutable.HashSet[String]()
        val uids: util.HashSet[String] = new util.HashSet
        val itemIds: util.HashSet[String] = new util.HashSet
        val events: util.ArrayList[String] = new util.ArrayList[String]()
        //创建标志位用来判断是否有浏览商品行为
        var bool = true
        breakable {
          iter.foreach(log => {
            events.add(log.evid)
            //判断用户是否浏览商品
            if ("clickItem".equals(log.evid)) {
              bool = false
              break()
            } else if ("coupon".equals(log.evid)) {
              uids.add(log.uid)
              itemIds.add(log.itemid)
            }
          })
        }
        //产生疑似预警日志
        (uids.size() >= 3 && bool, CouponAlertInfo(mid, uids, itemIds, events, System.currentTimeMillis()))
      }
      }
    })

    //8.生成预警日志
    val couponAlertDStream: DStream[CouponAlertInfo] = boolToCouponAlertDStream.filter(_._1).map(_._2)

    couponAlertDStream.print(100)
    //9.写入ES
    /*couponAlertDStream.foreachRDD(rdd=>{
      rdd.foreachPartition(iter=>{

        val list: List[(String, CouponAlertInfo)] = iter.toList.map(log => {
          val docId = log.mid + log.ts / 60000
          (docId, log)
        })
        MyEsUtil.insertBulk(GmallConstants.ES_ALERT_INDEXNAME,list)
      })
    })*/
    //10.开启任务
    ssc.start()
    ssc.awaitTermination()
  }

}
