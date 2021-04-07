package com.atguigu2.app

import java.text.SimpleDateFormat
import java.util

import com.alibaba.fastjson.JSON
import com.atguigu.GmallConstants
import com.atguigu.bean.{CouponAlertInfo, EventLog}
import com.atguigu.util.MyKafkaUtil
import com.atguigu.utils.MyEsUtil
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.kafka010.KafkaUtils
import org.apache.spark.streaming.{Minutes, Seconds, StreamingContext}

import scala.util.control.Breaks._

object AlertApp {
  def main(args: Array[String]): Unit = {
    val sparkConf: SparkConf = new SparkConf().setAppName("AlertApp").setMaster("local[*]")
    val ssc = new StreamingContext(sparkConf,Seconds(5))

    //需求：同一设备，5分钟内三次及以上用不同账号登录并领取优惠劵，并且过程中没有浏览商品。
    // 达到以上要求则产生一条预警日志。并且同一设备，每分钟只记录一次预警。

    //1、5分钟内 开窗
    //2、同一设备 groupByKey
    //3、三次及以上不同账号=> uid加入set集合
    val originDStream: InputDStream[ConsumerRecord[String, String]] = MyKafkaUtil.getKafkaStream(GmallConstants.KAFKA_TOPIC_EVENT,ssc)


    /*originDStream.foreachRDD(rdd=>{
      rdd.foreach(log=>{
        println(log.value())
      })
    })*/


    //val oriWindow: DStream[ConsumerRecord[String, String]] = originDStream.window(Minutes(5))

    //将数据转换为样例类
    val sdf = new SimpleDateFormat("yyyy-MM-dd HH")

    val midAndLogDStream: DStream[(String, EventLog)] = originDStream.mapPartitions(partition => {
      partition.map(record => {
        val eventLog: EventLog = JSON.parseObject(record.value(), classOf[EventLog])
        eventLog.logDate = sdf.format(eventLog.ts).split(" ")(0)
        eventLog.logHour = sdf.format(eventLog.ts).split(" ")(1)
        //因为我下面要groupByKey,所以返回元组)
        (eventLog.mid, eventLog)
      })
    })

    val midAndLogWindowDStream: DStream[(String, EventLog)] = midAndLogDStream.window(Minutes(5))
    //groupByKey
    val midAndLogIterDStream: DStream[(String, Iterable[EventLog])] = midAndLogWindowDStream.groupByKey()

    val boolAndLogDStream: DStream[(Boolean, CouponAlertInfo)] = midAndLogIterDStream.mapPartitions(partition => {
      partition.map {
        case (mid, iter) => {
          val uids = new util.HashSet[String]
          val itemIds = new util.HashSet[String]
          val events = new util.ArrayList[String] //为什么用list？
          var bool = true  //true默认为非法账号

          iter.foreach(log => {
            events.add(log.evid)
            breakable(
              if ("clickItem".equals(log.evid)) {
                bool = false
                break
              } else if ("coupon".equals(log.evid)) {
                uids.add(log.uid)
                itemIds.add(log.itemid)
              }

            )
          })
          //说明3次不同账号，仅领取优惠券
          /*if(uids.size()>=3 && bool){
            (mid,uids,itemIds,events,System.currentTimeMillis())
          }*/
          (uids.size() >= 3 && bool, CouponAlertInfo(mid, uids, itemIds, events, System.currentTimeMillis()))

        }
      }
    })
    //过滤出非法mid
    val InfoDStream: DStream[CouponAlertInfo] = boolAndLogDStream.filter(_._1).map(_._2)
    InfoDStream.count().print()
    InfoDStream.print(100)
    //写入ES
    InfoDStream.foreachRDD(rdd=>{
      rdd.foreachPartition(iter=>{
        val list: List[(String, CouponAlertInfo)] = iter.toList.map(log => {
          val docId = log.mid + ":" + log.ts / 60000
          (docId, log)
        })
        MyEsUtil.insertBulk(GmallConstants.ES_ALERT_INDEXNAME,list)
      })
    })


    ssc.start();
    ssc.awaitTermination();
  }
}
