package chapter3

/**
 * 功能 : 
 *
 * @date : 2021-09-08 10:18
 * @package : chapter3
 * @author : 谷燚
 * @email : 853779011@qq.com
 * @version : 1.0
 */
object Convenience {
  /**
   * scala从main()方法开始处理，强制程序入口
   * @param args
   */
  def main(args: Array[String]): Unit = {
    // 闭区间 1 to 3 等价于 1.to(3) ,但是前者更优雅
    for (i <- 1 to 3) {
      print(i)
      print(",")
    }
    // 左闭右开
    for (i <- 1 until 3){
      print(s"$i")
    }

  }
  /**
   * val 不可变性
   * var 可变性
   */
  def valAndVarTest(): Unit ={
    val buffer = new StringBuffer();
    buffer.append("funny")
    println(buffer)
    buffer.append("!")
    println(buffer)
  }

  /**
   *  便利性
   *  a + b --> a.+(b)
   *  1 to 3 --> 1.to(3)
   */
  object car{
    def turn(direction: String): Unit ={
      car turn "right"
    }
  }

}
