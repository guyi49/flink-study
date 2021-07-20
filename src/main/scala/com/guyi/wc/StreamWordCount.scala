package com.guyi.wc

import org.apache.flink.api.java.utils.ParameterTool
import org.apache.flink.streaming.api.scala._ //导入
/**
 * 功能 : 
 * date : 2021-01-06 15:26
 *
 * @author : 谷燚
 * @version : 1.0
 * @since : JDK 1.8
 */
// 流处理的word Count
object StreamWordCount {
  def main(args: Array[String]): Unit = {
    // 创建流处理的执行环境
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    // 返回本地执行环境，需要在调用时指定默认的并行度
//    var env = StreamExecutionEnvironment.createLocalEnvironment(1)
    // 	返回集群执行环境，将jar提交给远程服务器，需要在调用时指定JobManager的IP和端口号，并指定要在集群中运行的jar包
//    var env = StreamExecutionEnvironment.createRemoteEnvironment("jobManager-hostname",6123,"jarPath//wc.jar")
    // 修改并行度 默认是cpu核数 4 
//    env.setParallelism(8)

    // 从外部命令中提取参数，作为socket文本流的主机名和端口号
    val paramTool: ParameterTool = ParameterTool.fromArgs(args)
    val host: String = paramTool.get("host")
    val port: Int = paramTool.getInt("port")

    // 真正的流处理，数据应该是源源不断的

    // 接收一个socket文本流
    var inputDataStream: DataStream[String] = env.socketTextStream(host,port)

//    var inputDataStream: DataStream[String] = env.socketTextStream("10.200.20.155",7777)
//    var inputDataStream = env.readTextFile("D:\\vs\\mytools\\fun1\\test.txt")

    // 进行转换处理统计
    var resultDataStream: DataStream[(String, Int)] = inputDataStream
      .flatMap(_.split(" "))
      .filter(_.nonEmpty)
      .map((_, 1))
      .keyBy(0) // 指定一个key 分组
      .sum(1)

    // 打印输出
    resultDataStream.print().setParallelism(1)

    // 事件驱动，启动任务执行
    env.execute("stream word count")
  }

}
