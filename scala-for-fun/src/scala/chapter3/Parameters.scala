package chapter3

/**
 * 功能 : 
 *
 * @date : 2021-09-08 11:18
 * @package : chapter3
 * @author : 谷燚
 * @email : 853779011@qq.com
 * @version : 1.0
 */
object Parameters {
  // 在参数后面加*，代表参数可以接收变长参数值
  def max(values: Int*) = values.foldLeft(values(0)){Math.max}

  def function(input: Int*):Unit = println(input.getClass)

  def main(args: Array[String]): Unit = {
//    println(max()) Exception in thread "main" java.lang.IndexOutOfBoundsException: 0
    println(max(8, 2, 3))
    println(max(8, 2, 3,88,12,99,1,6))
    function(1,2,3) //class scala.collection.mutable.WrappedArray$ofInt
    val numbers = Array(2,5,3,7,1,6)
//    max(numbers) 类型匹配错误 Array[Int]
    // 使用数据展开标记
    println(max(numbers: _*))
  }

}
