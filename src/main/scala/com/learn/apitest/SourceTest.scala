package com.learn.apitest

import java.util.Properties

import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.api.scala._
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer011

case class SensorReading(id:String,timestamp:Long,temperature:Double)

object SourceTest {
  def main(args: Array[String]): Unit = {
    val env=StreamExecutionEnvironment.getExecutionEnvironment

    //1.从自定义的集合中读取
    val stram=env.fromCollection(List(
      SensorReading("12345",123213L,36.2),
      SensorReading("123456",123213L,36.2)
      ,SensorReading("123457",123213L,36.2)
    ))


    //2.从kafka中读取数据

    val properties=new Properties()
    properties.setProperty("bootstrap.servers","192.168.2.160:23092")
    properties.setProperty("group.id","test-kafka")
    properties.put("enable.auto.commit", "true")
    properties.put("auto.commit.interval.ms", "1000")
    properties.put("session.timeout.ms", "30000")
    properties.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
    properties.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
    properties.setProperty("auto.offset.reset", "latest")
    val stream2=env.addSource(new FlinkKafkaConsumer011[String]("sendor",new SimpleStringSchema(),properties));


    stream2.print("stram").setParallelism(5)
    env.execute("source test")

  }
}
