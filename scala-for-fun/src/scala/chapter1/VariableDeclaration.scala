package chapter1

/**
 * 功能 : 
 *
 * @date : 2021-09-08 10:46
 * @package : chapter1
 * @author : 谷燚
 * @email : 853779011@qq.com
 * @version : 1.0
 */
/**
 * val 不可变性 类似于final，在scala用法中对于变量最好用val去约束。
 * var 可变性
 */
object VariableDeclaration {
  def main(args: Array[String]): Unit = {
    var x = 10
    val myString = "hello,scala"
    // myString = "hello,spark and flink!"

    // 变量类型声明
    var y : Int = 100

    // 多个变量声明
    val a,b = 100
    println(b)
    println(a)
    val myTuple = (23,"scala")
    println(myTuple)
  }

}
