package chapter3

/**
 * 功能 : 
 *
 * @date : 2021-09-08 11:27
 * @package : chapter3
 * @author : 谷燚
 * @email : 853779011@qq.com
 * @version : 1.0
 */
object ParametersDefaultValues {
  def mail(destination:String = "head office",mailClass:String = "first"):Unit =
    println(s"sending to $destination by $mailClass class")

  def main(args: Array[String]): Unit = {
    mail("Houston office","Priority")
    mail("Houston office") // 被省去的参数所使用的位置，是由位置决定的
    mail()
    /*
     result :
     sending to Houston office by Priority class
     sending to Houston office by first class
     sending to head office by first class
     */


  }
}
