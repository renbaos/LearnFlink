package com.learn.apitest

import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.time.Time

object WindowTest {

  def main(args: Array[String]): Unit = {
    val environment=StreamExecutionEnvironment.getExecutionEnvironment

//    val txt: DataStream[String] = environment.readTextFile("F://a.txt")

    val txt=environment.socketTextStream("localhost",7777)

    val dataStream= txt.map(row => {
      val arr: Array[String] = row.split(",")

      SensorReading(arr(0).trim, arr(1).toLong, arr(2).toDouble)
    })

    val minTemp =dataStream.map(data=>{

      (data.id,data.temperature)
    }).keyBy(_._1).timeWindow(Time.seconds(10))
      .reduce((data1,data2)=>(data1._1,data1._2.min(data2._2)))


    minTemp.print()

    dataStream.print("input data")

    environment.execute()
  }
}
