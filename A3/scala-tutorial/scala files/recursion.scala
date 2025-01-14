object recursion
{
  // Function define
  def fact(n:Int): Int=
  {
    if(n == 1) 1
    else n * fact(n - 1)
  }

  // Main method
  def main(args:Array[String])
  {
    println(fact(3))
  }
}