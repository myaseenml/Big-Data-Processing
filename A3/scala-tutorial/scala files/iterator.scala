object Iter {
  def main(args: Array[String]) {
    val it = Iterator("a", "number", "of", "words",'h')

    while (it.hasNext){
      var a =it.next()
      print(a)
      //,it.next())
    }
  }
}