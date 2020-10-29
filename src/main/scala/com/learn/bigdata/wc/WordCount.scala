package com.learn.bigdata.wc

import org.apache.flink.api.scala.{DataSet, ExecutionEnvironment}
import org.apache.flink.api.scala._
object WordCount {
  def main(args: Array[String]): Unit = {
    //创建一个批处理的执行环境
       val environment: ExecutionEnvironment = ExecutionEnvironment
         .getExecutionEnvironment

    //从文件中读取数据
    val inputPath="D:\\soft\\ideaworkspace\\learn\\LearnFlink\\src\\main\\resources\\hello.txt";

    val text: DataSet[String] = environment.readTextFile(inputPath)

    val wordCount=text.flatMap(_.split(" ")).map( (_,1) ).groupBy(0).sum(1)

    wordCount.print()

  }
}
