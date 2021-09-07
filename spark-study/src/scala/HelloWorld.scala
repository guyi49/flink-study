/**
 * 功能 : 
 *
 * @date : 2021-09-07 10:53
 * @package : 
 * @author : 谷燚
 * @email : 853779011@qq.com
 * @version : 1.0
 */
object HelloWorld {
//
//  def main(args: Array[String]): Unit = {
//    println("hello world")
//  }

  /**
   *  值与变量
   *  val 赋值后不可变，类比于Java中的 final，值一旦初始化就不能再改变。
   *  var 赋值后可以改变，生命周期中可以被多次赋值
   */
  val money = 100

  def main(args: Array[String]): Unit = {
    print(money)
  }
}
