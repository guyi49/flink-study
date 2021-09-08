package chapter3

/**
 * 功能 : 
 *
 * @date : 2021-09-08 11:05
 * @package : chapter3
 * @author : 谷燚
 * @email : 853779011@qq.com
 * @version : 1.0
 */
object MultipleAssignment {
  def getPersonInfo(pKey:Int) = {
    // 假定pKey是用来获取用户信息的主键
    // 响应体是固定的
    ("yi","gu","yigu@gudu.com")
  }

  def main(args: Array[String]): Unit = {
    val (firstName,lastName,emailAddress) = getPersonInfo(1)
    println(s"first Name: $firstName")
    println(s"last Name: $lastName")
    println(s"email address: $emailAddress")
  }


}
