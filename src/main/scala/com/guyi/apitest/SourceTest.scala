package com.guyi.apitest

import java.util.Properties

import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.streaming.api.functions.source.SourceFunction
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer
import sun.nio.cs.ext.DoubleByteEncoder

import scala.util.Random

/**
 * 功能 : 
 * date : 2021-03-05 10:55
 * package : com.guyi.apitest
 *
 * @author : 谷燚
 * @version : 1.0
 */
// 定义样例类，温度传感器
case class SensorReading(id: String, timestamp: Long, temperature: Double)

object SourceTest {
  def main(args: Array[String]): Unit = {
    // 创建执行环境
    val env = StreamExecutionEnvironment.getExecutionEnvironment

    // 1. 从集合中读取数据
    val dataList = List(
      SensorReading("sensor1",1547718199,35.8),
      SensorReading("sensor2",1547719999,20),
      SensorReading("sensor3",1547720000,15),
      SensorReading("sensor4",1547720050,14),
      SensorReading("sensor5",1547720150,9),
      SensorReading("sensor6",1547720190,20)
    )
    val stream1 = env.fromCollection(dataList)

//    stream1.print()

//    env.fromElements(1.0,35,"hello")

    // 2. 从文件中读取数据
    val inputPath = "D:\\homeinnsProject\\study-flink\\src\\main\\resources\\sensor.txt"

    val stream2 = env.readTextFile(inputPath)

//    stream2.print()

    // 3. 从kafka中读取数据 此时作为flink作为消费
    val properties = new Properties()
    properties.setProperty("bootstrap.servers","10.200.20.153:9092")
//    properties.setProperty("group.id","test-consumer-group")

//    properties.setProperty("group.id", "group_test")
//    properties.setProperty("auto.offset.reset","earliest")

    val stream3 = env.addSource(new FlinkKafkaConsumer[String]("sensor",new SimpleStringSchema(), properties))
//    stream3.print()

    // 4. 自定义Source
//    val stream4 = env.addSource(new MySensorSource())

    stream3.print().setParallelism(1)
    env.execute("source test")
  }
}
// 自定义sourceFunction
//class MySensorSource() extends SourceFunction[SensorReading]{
//  // 定义一个标志位flag，用来表示数据源是否正常运行发出数据
//  var running: Boolean = true
//  override def cancel(): Unit = running = false
//
//  override def run(sourceContext: SourceFunction.SourceContext[SensorReading]): Unit = {
//    // 定义一个随机数发生器
//    var rand = new Random()
//
//    // 随机生成一组（10个）传感器的初始温度：（id,temp）
////    1.to(10).map(i => ("sensor_" + i ,))
//
//    // 定义无线循环，不停地产生数据，除非被cancel
//    while(running){
//
//    }
//  }
//}
