class obj
{
  var i:Int=10

  // function 1 with two parameters
  def fun(p:Int, q:Int)
  {
    var Sum = p + q;
    println("Sum in function 1 is:" + Sum);
  }

  // function 2 with three parameters
  def fun(p:Int, q:Int, r:Int)
  {
    var Sum = p + q + r;
    println("Sum in function 2 is:" + Sum);
  }
}


object Main1
{
  // Main function
  def main(args: Array[String])
  {


    var obj1=new obj;
    obj1.fun(3,4)
    obj1.fun(3,4,5)

  }
}