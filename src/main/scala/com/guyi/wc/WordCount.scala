package com.guyi.wc

import org.apache.flink.api.scala.{DataSet, ExecutionEnvironment}
import org.apache.flink.api.scala._

/**
 * 功能 : 
 * date : 2021-01-06 14:38
 *
 * @author : 谷燚
 * @version : 1.0
 * @since : JDK 1.8
 */
// 批处理的word count
object WordCount {
  def main(args: Array[String]): Unit = {
    // 创建一个批处理的执行环境
    var env: ExecutionEnvironment = ExecutionEnvironment.getExecutionEnvironment // 注意选择是java还是Scala的api

    // 从文件中读取数据
    var inputPath: String = "D:\\homeinnsProject\\study-flink\\src\\main\\resources\\hello.txt"
    var inputDataSet = env.readTextFile(inputPath)

    // 对数据进行转换处理统计，先分词，再按照word进行分组，最后聚合统计
    var resultDataSet: DataSet[(String, Int)] = inputDataSet
      .flatMap(_.split(" "))
      .map((_, 1))
      .groupBy(0) // 以第一个元素作为key，进行分组统计
      .sum(1)

    // 打印输出
    resultDataSet.print()
  }
}
