package com.learn.apitest.sink

import com.learn.apitest.SensorReading
import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer011

object KafkaSink {
  def main(args: Array[String]): Unit = {
    val environment=StreamExecutionEnvironment.getExecutionEnvironment

    val txt: DataStream[String] = environment.readTextFile("F://a.txt")

    val dataStream: DataStream[String] = txt.map(row => {
      val arr: Array[String] = row.split(",")

      SensorReading(arr(0).trim, arr(1).toLong, arr(2).toDouble).toString
    })
    dataStream

    dataStream.addSink(new FlinkKafkaProducer011[String]("192.168.2.160:23092","test",new SimpleStringSchema()))
    environment.execute("execute kafka test")
  }
}
