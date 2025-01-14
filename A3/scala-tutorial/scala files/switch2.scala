object switch {
  //def main(args: Array[String]) {
    val a=10
    var b=20
    print(a)
    print(doTest(2))
    print(doTest("two"))
    print(doTest(1))
//  }

  def doTest(x: Any): Any = x match {
    case _ => "many"
    case 1 => "one"
    case "two" => 2

  }
}