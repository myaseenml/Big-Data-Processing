object Main extends App {

  //List

  var list1=List(1,2,3,4)
  list1.foreach{i => println(i)}
  println(list1)
  var list2=list1.reverse
  println(list1)
  print(list2)

}
