object Set1 {
  def main(args: Array[String]) {
    var num1 = Set(5,5,6,9,20,30,45)
    //add element
    print(num1.size)
    num1=num1+60
    val num2 = Set(50,60,9,20,35,55)
    print(num1)

    // find common elements between two sets
    //println( "num1.&(num2) : " + num1.&(num2) )
    //println( "num1.intersect(num2) : " + num1.intersect(num2) )
  }
}