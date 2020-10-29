package com.learn.apitest.sink

import com.learn.apitest.SensorReading
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.connectors.redis.RedisSink
import org.apache.flink.streaming.connectors.redis.common.config.FlinkJedisPoolConfig
import org.apache.flink.streaming.connectors.redis.common.mapper.{RedisCommand, RedisCommandDescription, RedisMapper}

object RedisSink {
  def main(args: Array[String]): Unit = {
    val env=StreamExecutionEnvironment.getExecutionEnvironment


    val txt: DataStream[String] = env.readTextFile("F://a.txt")

    val dataStream: DataStream[SensorReading] = txt.map(row => {
      val arr: Array[String] = row.split(",")

      SensorReading(arr(0).trim, arr(1).toLong, arr(2).toDouble)
    })

    val conf=new FlinkJedisPoolConfig.Builder()
      .setPort(6378).setHost("192.168.2.141").build()

    dataStream.addSink(new RedisSink(conf,new MyRedisMapper))


   env.execute("redis")
  }
}
class MyRedisMapper() extends RedisMapper[SensorReading]{
  override def getCommandDescription: RedisCommandDescription = {
    new RedisCommandDescription(RedisCommand.HSET,"sensor_temperature")
  }

  override def getValueFromData(t: SensorReading): String = t.temperature.toString

  override def getKeyFromData(t: SensorReading): String = t.id
}