package com.atguigu.handler

import java.{lang, util}
import java.text.SimpleDateFormat
import java.util.Date

import com.atguigu.bean.StartUpLog
import com.atguigu.util.RedisUtil
import org.apache.spark.SparkContext
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.dstream.DStream
import redis.clients.jedis.Jedis

object DauHandler {
  //批次内去重，我们需要得到当日日活，同一台设备。
  // 总思路是，转换数据结构=>groupBy=>按时间排序=>取第一个
  def filterByGroup(filterByGroupDStream: DStream[StartUpLog]) = {
      //1、将数据转化为k,v  ((mid,logDate),log)  要聚合当日的同一台设备
      //需要返回值，使用map相关算子，使用mapPartitions提高并行度
      val midAndDateToLogDStream: DStream[((String, String), StartUpLog)] = filterByGroupDStream.mapPartitions(partition => {
        partition.map(log => {
          ((log.mid, log.logDate), log)
        })
      })


      //2、groupByKey将相同key的数据聚合到同一个分区中
      val midAndDateToLogInterDStream: DStream[((String, String), Iterable[StartUpLog])] = midAndDateToLogDStream.groupByKey()

      //3、将数据排序并取第一条数据
      val midAndDateToLogListDStream: DStream[((String, String), List[StartUpLog])] = midAndDateToLogInterDStream.mapValues(iter => {
        iter.toList.sortWith(_.ts < _.ts).take(1)
      })
      midAndDateToLogListDStream.flatMap(_._2)
  }


  def filterByRedis(startUpLogDStream: DStream[StartUpLog],sc: SparkContext) = {
    //方案一，对进来的每一条数据过滤
    //每一条数据都需要开关连接，效率低
//    val value: DStream[StartUpLog] = startUpLogDStream.filter(log => {
//      //创建redis连接
//      val jedisClient: Jedis = RedisUtil.getJedisClient
//
//      //rediskey
//      val rediskey = "DAU:" + log.logDate
//      //对比数据，重复的去掉，不重的留下来
//      val boolean: lang.Boolean = jedisClient.sismember(rediskey, log.mid)
//      //关闭连接
//      jedisClient.close()
//      !boolean
//    })
//    value
    //方案二：在分区下创建连接(优化)
    /*val value: DStream[StartUpLog] = startUpLogDStream.mapPartitions(partition => {
      //1、创建redis连接
      val jedisClient: Jedis = RedisUtil.getJedisClient

      //2、过滤
      val logs: Iterator[StartUpLog] = partition.filter(log => {
        val rediskey = "DAU:" + log.logDate
        val boolean: Boolean = jedisClient.sismember(rediskey, log.mid)
        !boolean
      })

      //3、关闭连接
      jedisClient.close()
      logs
    })
    value*/

    //方案三：在每个批次内创建一次连接，来优化连接个数
    //transform能对批次进行操作？
    val sdf: SimpleDateFormat = new SimpleDateFormat("yyyy-MM-dd")
    val value = startUpLogDStream.transform(rdd => {
      //1、获取redis连接
      val jedisClient: Jedis = RedisUtil.getJedisClient
      //2、查redis中mid
      //获取rediskey
      // val rediskey = "DAU:" + log.logDate 因为啊，这边是以批次的形式，是无法获取log的
      val rediskey = "DAU:" + sdf.format(new Date(System.currentTimeMillis()))

      val mids = jedisClient.smembers(rediskey)

      //3.将数据广播至executor端？为什么要广播？为什么会想到广播呢
      //因为mids是在driver端，我们要在executor端使用driver端的数据
      val midBC: Broadcast[util.Set[String]] = sc.broadcast(mids)

      //4、根据获取到的mid去重
      val midFilterRDD: RDD[StartUpLog] = rdd.filter(log => {
        !midBC.value.contains(log.mid)
      })
      //关闭连接
      jedisClient.close()
      midFilterRDD
    })
    value
  }

  /**
    * 将去重后的数据保存至Redis，为了下一批数据去重用
    * @param filterByGroupDStream
    */
  def saveMidToRedis(startUpLogDStream: DStream[StartUpLog]) = {
    startUpLogDStream.foreachRDD(rdd=>{
      rdd.foreachPartition(paritition=>{
        //1、创建连接
        val jedisClient: Jedis = RedisUtil.getJedisClient
        //2、写库
        paritition.foreach(log=>{
          //rediskey
          val rediskey = "DAU:" + log.logDate
          //将mid存入redis
          jedisClient.sadd(rediskey,log.mid)
        })
        //关闭连接
        jedisClient.close()
      })
    })
  }

}
