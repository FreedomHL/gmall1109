package com.atguigu2.handler

import java.text.SimpleDateFormat
import java.util

import com.atguigu.bean.StartUpLog
import com.atguigu.util.RedisUtil
import org.apache.spark.SparkContext
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.dstream.DStream
import redis.clients.jedis.Jedis

object DauHandler {
  //批次内去重，需要返回值
  def filterByGroup(filterByResdisDStream: DStream[StartUpLog]) = {
    //1、groupByKey分组
    val value: DStream[((String, String), StartUpLog)] = filterByResdisDStream.mapPartitions(partition => {
      partition.map(log => {
        ((log.logDate, log.mid), log)
      })
    })
    val dateAndMidtoLog: DStream[((String, String), Iterable[StartUpLog])] = value.groupByKey()

    //2、排序取第一个
    val result: DStream[((String, String), List[StartUpLog])] = dateAndMidtoLog.mapValues(iter => {
      iter.toList.sortWith(_.ts < _.ts).take(1)
    })
    result.flatMap(_._2)
  }

  //批次间去重 。去重后的结果要作为参数给批次内 去去重。因此，需要返回值
  def filterByRedis(startUpLogDStream: DStream[StartUpLog],sc : SparkContext) = {
    //1、单个rdd 触发一次redis显然不靠谱
    //2、以分区的形式，redis连接还是过多
    //3、那就以批次的形式
    /*val value: DStream[StartUpLog] = startUpLogDStream.transform(rdd => {
      val jedisClient: Jedis = RedisUtil.getJedisClient

      rdd.filter(
        log => {
          val redisKey = "DAU:" + log.logDate
          !jedisClient.smembers(redisKey).contains(log.mid)

      })
    })
    value*/

    val sdf = new SimpleDateFormat("yyyy-MM-dd")

    val value: DStream[StartUpLog] = startUpLogDStream.transform(rdd => {
      //这里的代码在driver端执行，每个批次执行一次
      //1、创建redis连接
      val jedisClient: Jedis = RedisUtil.getJedisClient

      //2、获取redis中的mids
      //val rediskey = "DAU:" + log.logDate

      val redisKey = "DAU:" + sdf.format(System.currentTimeMillis())

      val mids: util.Set[String] = jedisClient.smembers(redisKey)

      val midBC: Broadcast[util.Set[String]] = sc.broadcast(mids)
      //3、去重, 把不和redis重复的返回
      val value: RDD[StartUpLog] = rdd.filter(log => {
        //这里在executor端执行
        !midBC.value.contains(log.mid)
      })
      jedisClient.close()
      value

    })
    value


  }





  def saveToRedis(startUpLogDStream: DStream[StartUpLog]) = {
    startUpLogDStream.foreachRDD(rdd=>{
      //println("5秒一次")
      rdd.foreachPartition(partition=>{
        val jedisClient: Jedis = RedisUtil.getJedisClient

        partition.foreach(log=>{
          val redisKey = "DAU:" + log.logDate
          jedisClient.sadd(redisKey,log.mid)
        })

        //每一个分区关一次
        jedisClient.close()
      })
    })

  }

}
