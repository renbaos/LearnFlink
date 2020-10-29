package com.learn.apitest.sink

import java.sql.{Connection, DriverManager, PreparedStatement}

import com.learn.apitest.SensorReading
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction
import org.apache.flink.streaming.api.scala._

object JDBCSinkTest {
  def main(args: Array[String]): Unit = {
    val environment=StreamExecutionEnvironment.getExecutionEnvironment


    val txt: DataStream[String] = environment.readTextFile("F://a.txt")

    val dataStream:DataStream[SensorReading]= txt.map(row => {
      val arr: Array[String] = row.split(",")

      SensorReading(arr(0).trim, arr(1).toLong, arr(2).toDouble)
    })

    dataStream.addSink(new MyJDBCSink)

    environment.execute("jdbc test")

  }


}

class MyJDBCSink() extends RichSinkFunction[SensorReading]{

  var conn:Connection= _

  var insertStmt:PreparedStatement = _

  var updateStmt:PreparedStatement = _


  override def open(parameters: Configuration): Unit ={
    super.open(parameters)

    conn = DriverManager.getConnection("jdbc:mysql://127.0.0.1:3306/test","root","root")

    insertStmt =conn.prepareStatement("insert into temperatures (id,temp) values (?,?)")

    updateStmt = conn.prepareStatement("update temperatures set temp = ? where id= ?")



  }

  override def invoke(value: SensorReading): Unit = {
    updateStmt.setDouble(1,value.temperature)
    updateStmt.setString(2,value.id)
    updateStmt.execute()

    if(updateStmt.getUpdateCount == 0){
      insertStmt.setString(1,value.id)
      insertStmt.setDouble(2,value.temperature)
      insertStmt.execute()
    }
  }

  override def close(): Unit = {
    updateStmt.close()

    insertStmt.close()
    conn.close()
  }
}
