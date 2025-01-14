import Array._

object Multi {


  def main(args: Array[String]) {

    var myMatrix = ofDim[Int](3, 3)

    // build a matrix
    for (i <- 0 to 2) {
      for (j <- 0 to 2) {
        myMatrix(i)(j) = j;
      }
    }

    for (i <- 0 to 2) {
      for (j <- 0 to 2) {
        println(myMatrix(i)(j))
      }
    }


  }
}