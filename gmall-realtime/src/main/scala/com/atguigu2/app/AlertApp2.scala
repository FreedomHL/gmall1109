package com.atguigu2.app

import com.atguigu.GmallConstants
import com.atguigu.util.MyKafkaUtil
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.InputDStream
import org.apache.spark.streaming.{Seconds, StreamingContext}

object AlertApp2 {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("alertApp")
    val ssc = new StreamingContext(conf,Seconds(5))

    val originDStream: InputDStream[ConsumerRecord[String, String]] = MyKafkaUtil.getKafkaStream(GmallConstants.KAFKA_TOPIC_EVENT,ssc)

    originDStream.foreachRDD(rdd=>{
      rdd.foreach(log=>{
        println(log)
      })
    })

    /*originDStream.foreachRDD(rdd=>{
      /*rdd.foreachPartition(iter=>{
        iter.foreach(println)
      })*/
      rdd.foreach(record=>{
        println(record)
      })
    })*/


    ssc.start()
    ssc.awaitTermination()
  }
}
