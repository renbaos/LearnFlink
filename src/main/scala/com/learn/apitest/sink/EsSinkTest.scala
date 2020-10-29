package com.learn.apitest.sink

import java.util

import com.learn.apitest.SensorReading
import org.apache.flink.api.common.functions.RuntimeContext
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.connectors.elasticsearch.{ElasticsearchSinkFunction, RequestIndexer}
import org.apache.flink.streaming.connectors.elasticsearch6.ElasticsearchSink
import org.apache.http.HttpHost
import org.elasticsearch.client.Requests

object EsSinkTest {
    def main(args: Array[String]): Unit = {
      val environment=StreamExecutionEnvironment.getExecutionEnvironment

      val txt: DataStream[String] = environment.readTextFile("F://a.txt")

      val dataStream= txt.map(row => {
        val arr: Array[String] = row.split(",")

        SensorReading(arr(0).trim, arr(1).toLong, arr(2).toDouble)
      })

      val httpHost=new util.ArrayList[HttpHost]()


      httpHost.add(new HttpHost("192.168.2.160",9200))


      val esSinkBuilder=new ElasticsearchSink.Builder[SensorReading](httpHost,
        new ElasticsearchSinkFunction[SensorReading] {
          override def process(t: SensorReading, runtimeContext: RuntimeContext, requestIndexer: RequestIndexer)
          : Unit = {
            println("commit a data"+t)
            val json=new util.HashMap[String,String]()
            json.put("id",t.id)
            json.put("temp",t.temperature.toString)
            json.put("ts",t.timestamp.toString)


            val indexRequest=Requests.indexRequest().index("sensor").`type`("readingdata").source(json)

            requestIndexer.add(indexRequest)

            print("finished !!")
          }
        }
      )

      val value: ElasticsearchSink[SensorReading] = esSinkBuilder.build()

      dataStream.addSink(esSinkBuilder.build())

      environment.execute("execute kafka test")
    }
}
