package com.atguigu.app

import java.text.SimpleDateFormat
import java.util.Date

import com.alibaba.fastjson.JSON
import com.atguigu.GmallConstants
import com.atguigu.bean.StartUpLog
import com.atguigu.handler.DauHandler
import com.atguigu.util.MyKafkaUtil
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.{Seconds, StreamingContext}

object DauApp {
  def main(args: Array[String]): Unit = {
    //TODO 1.创建sparkConf
    val sparkConf: SparkConf = new SparkConf().setAppName("DauApp").setMaster("local[*]")

    //TODO 2.创建StreamingContext
    val ssc = new StreamingContext(sparkConf,Seconds(5))

    //TODO 3.连接kafka
    //为什么会变成kv，kv分别是什么？k是时间戳？v json串？
    val kafkaDStream: InputDStream[ConsumerRecord[String, String]] = MyKafkaUtil.getKafkaStream(GmallConstants.KAFKA_TOPIC_STARTUP,ssc)

    //TODO 4.将json数据转换为样例类，方便解析
    //需要返回值
    val sdf = new SimpleDateFormat("yyyy-MM-dd HH")

    val startUpLogDStream: DStream[StartUpLog] = kafkaDStream.mapPartitions(partition => {
      partition.map(record => {
        val startUpLog: StartUpLog = JSON.parseObject(record.value(), classOf[StartUpLog])
        val str: String = sdf.format(new Date(startUpLog.ts))

        //补充字段 logdate yyyy-MM-dd
        startUpLog.logDate = str.split(" ")(0)

        //补充字段 loghour HH
        startUpLog.logHour = str.split(" ")(1)
        startUpLog

      })
    })
    //startUpLogDStream
    //TODO 5.进行批次间去重
    //优先批次间去重，因为批次间能过滤的数据更多，等到批次内的时候，就少些计算了
    val filterByRedisDStream = DauHandler.filterByRedis(startUpLogDStream,ssc.sparkContext)

    startUpLogDStream.count().print()
    filterByRedisDStream.count().print()
    //TODO 6.进行批次内去重
    val filterByGroupDStream: DStream[StartUpLog] = DauHandler.filterByGroup(filterByRedisDStream)


    //TODO 7.将去重后的数据保存至Redis，为了下一批数据去重用
    DauHandler.saveMidToRedis(filterByGroupDStream)
    //TODO 8.将数据保存至Hbase

    //测试kafka连接
    /*kafkaDStream.foreachRDD(rdd=>{
      rdd.foreach(record=>{
        println(record.key())
        println(record.value())
      })
    })*/
    ssc.start()
    ssc.awaitTermination()
  }
}
