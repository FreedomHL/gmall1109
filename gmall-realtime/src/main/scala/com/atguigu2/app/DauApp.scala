package com.atguigu2.app

import java.text.SimpleDateFormat

import com.alibaba.fastjson.JSON
import com.atguigu.GmallConstants
import com.atguigu.bean.StartUpLog
import com.atguigu.util.{MyKafkaUtil, PropertiesUtil}
import com.atguigu2.handler.DauHandler
import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.streaming.{Seconds, StreamingContext}

object DauApp {
  def main(args: Array[String]): Unit = {
    //目的，实时计算日活
    //0、创建sparkstreaming环境
    val conf: SparkConf = new SparkConf().setAppName("DauApp2").setMaster("local[*]")
    val ssc = new StreamingContext(conf,Seconds(5))
    //1、连接kafka
    val originDstream: InputDStream[ConsumerRecord[String, String]] = MyKafkaUtil.getKafkaStream(GmallConstants.KAFKA_TOPIC_STARTUP,ssc)

    //现在还是json格式，我们利用redis去重，那么一定要有key和value
    //            key就是当前时间,value是mid
    //我们需要先解析json为bean

    val sdf = new SimpleDateFormat("yyyy-MM-dd HH")
    val startUpLogDStream: DStream[StartUpLog] = originDstream.mapPartitions(partition => {
      partition.map(record => {
        val value = JSON.parseObject(record.value(), classOf[StartUpLog])
        value.logDate = sdf.format(value.ts).split(" ")(0)
        value.logHour = sdf.format(value.ts).split(" ")(1)
        value
      })
    })

    //批次间去重
    val filterByResdisDStream: DStream[StartUpLog] = DauHandler.filterByRedis(startUpLogDStream,ssc.sparkContext)

    startUpLogDStream.count().print()
    filterByResdisDStream.count().print()

    //批次内去重
    val filterByGroupDstream= DauHandler.filterByGroup(filterByResdisDStream)

    //加入到redis中
    DauHandler.saveToRedis(filterByGroupDstream)
    import org.apache.phoenix.spark._
    //TODO 8.将数据保存至Hbase
    filterByGroupDstream.foreachRDD(rdd => {
      rdd.saveToPhoenix(
        "GMALL1109_DAU",
        Seq("MID", "UID", "APPID", "AREA", "OS", "CH", "TYPE", "VS", "LOGDATE", "LOGHOUR", "TS"),
        HBaseConfiguration.create,
        Some("hadoop102,hadoop103,hadoop104:2181"))
    })
    print("")
    ssc.start()
    ssc.awaitTermination()
  }
}
