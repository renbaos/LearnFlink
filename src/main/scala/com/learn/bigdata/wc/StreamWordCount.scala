package com.learn.bigdata.wc

import org.apache.flink.api.java.utils.ParameterTool
import org.apache.flink.api.scala._
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}

object StreamWordCount {
  def main(args: Array[String]): Unit = {

    val tool: ParameterTool = ParameterTool.fromArgs(args)

    val host: String = tool
      .get("host")

    val port: Int = tool.getInt("port")

    val environment: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment

    //接收socket数据流
    val textDataStream: DataStream[String] = environment
      .socketTextStream(host,port)


     //逐一读取数据

    val result: DataStream[(String, Int)] = textDataStream
      .flatMap(_.split(" ")).filter(_.nonEmpty)
      .map((_, 1)).keyBy(0).sum(1)
    result

    //执行任务
    result.print()
    environment.execute("start to learn a word count")

  }
}
