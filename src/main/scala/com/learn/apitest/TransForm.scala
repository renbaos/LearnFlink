package com.learn.apitest

import org.apache.flink.api.common.functions.FilterFunction
import org.apache.flink.streaming.api.scala._
object TransForm {
  def main(args: Array[String]): Unit = {
    val env=StreamExecutionEnvironment.getExecutionEnvironment
    val txt: DataStream[String] = env.readTextFile("F://a.txt")

    val dataStream: DataStream[SensorReading] = txt.map(row => {
      val arr: Array[String] = row.split(",")

      SensorReading(arr(0).trim, arr(1).toLong, arr(2).toDouble)
    })


    val splitStream: SplitStream[SensorReading] = dataStream.split(data => {

      if (data.temperature > 30) {
        Seq("high")
      } else {
        Seq("low")
      }
    })


    val high: DataStream[SensorReading] = splitStream
      .select("high")


    val low: DataStream[SensorReading] = splitStream.select("low")

    //合并两条流

    val warnning: DataStream[(String, Double)] = high.map(data=>(data.id,data.temperature))


    val connect: ConnectedStreams[(String, Double), SensorReading] = warnning.connect(low)


    val colMap: DataStream[Product with Serializable] = connect.map(

      warnningData => (warnningData._1, warnningData._2, "warinning"),
      lowData => (lowData.id,lowData.temperature, "healhy")
    )
//    colMap.print()


   val value: DataStream[SensorReading] = high.union(low)

    value.filter(new MyFilter).print()
    value.filter(data=>{
      val bool: Boolean = data.id.startsWith("aaa")
      bool
    })

    env.execute()






    //合并

  }
}

class MyFilter extends FilterFunction[SensorReading]{
  override def filter(t: SensorReading): Boolean = {
    t.id.startsWith("he")
  }
}