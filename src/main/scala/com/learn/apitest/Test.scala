package com.learn.apitest

import org.apache.flink.streaming.api.scala._

object Test {
  def main(args: Array[String]): Unit = {

    val env=StreamExecutionEnvironment.getExecutionEnvironment

    val unit: DataStream[(String, Int, Double)] = env.fromElements(("aaa",111,90.1))

    unit.print()
    env.execute("hi")
  }
}
