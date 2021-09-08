package chapter3

/**
 * 功能 : 
 *
 * @date : 2021-09-08 11:32
 * @package : chapter3
 * @author : 谷燚
 * @email : 853779011@qq.com
 * @version : 1.0
 */
/**
 *  Notes：
 *  1. 对于所有没有默认值的参数，必须要提供参数的值
 *  2. 对于那些有默认值的参数，可以选择地使用命名参数船只
 *  3. 一个参数最多只能传值一次
 *  4. 在重载基类地方法，应该保持参数名字地一致性。如果不这样做，编译器就会优先使用基类中的参数名，就可能会违背最初的目的
 *  5. 如果有多个重载的方法，它们的参数名一样，但是参数类型不同，那么函数调用就可能产生歧义；编译报错，不得不切换基于位置的参数形式。
 */
object ParametersNamed {
  // pow(2,3) 幂和基数无法判断 --> power(base = 2, exponent = 3)
  def mail(destination:String = "head office",mailClass:String = "first"):Unit =
    println(s"sending to $destination by $mailClass class")

  def main(args: Array[String]): Unit = {
    // pow(2,3) 幂和基数无法判断 --> power(base = 2, exponent = 3)
    // 使用命名参数来改写mail方法
    mail(mailClass = "Priority",destination = "Bahamas office")
    mail(mailClass = "Priority")
  }

}
