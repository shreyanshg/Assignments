package rotatematrix

/**
  * Created by Shreyansh Gupta on 2019-04-11
  */
class RotateMatrix{

  val N = 4
  def rotate90(a: Array[Array[Int]], order: String): Array[Array[Int]] = {

    order match {
      case "clockwise" =>
        var i = 0
        while(i < N/2) {
          var j = i
          while(j < N-i-1) {
            // Swap elements of each cycle
            // in clockwise direction
            val temp = a(i)(j)
            a(i)(j) = a(N - 1 - j)(i)
            a(N - 1 - j)(i) = a(N - 1 - i)(N - 1 - j)
            a(N - 1 - i)(N - 1 - j) = a(j)(N - 1 - i)
            a(j)(N - 1 - i) = temp
            j += 1
          }
          i += 1
        }
        a
      case "anti-clockwise" =>
        var i = 0
        while(i < N/2) {
          var j = i
          while(j < N-i-1) {
            // Swap elements of each cycle
            // in clockwise direction
            val temp = a(i)(j)
            a(i)(j) = a(j)(N - 1 - i)
            a(j)(N - 1 - i) = a(N - 1 - i)(N - 1 - j)
            a(N - 1 - i)(N - 1 - j) = a(N - 1 - j)(i)
            a(N - 1 - j)(i) = temp
            j += 1
          }
          i += 1
        }
        a
    }

  }

  def printMatrix(a: Array[Array[Int]]): Unit = {

    var i = 0
    while ( {
      i < N
    }) {
      var j = 0
      while ( {
        j < N
      }) {
        print(a(i)(j) + " ")
        j += 1
      }
      System.out.println()

      {
        i += 1
      }
    }

  }

}

object RotateMatrix {

  def main(args: Array[String]): Unit = {
    val rotateMatrix = new RotateMatrix
    val arr = Array(Array(1, 2, 3, 4), Array(5, 6, 7, 8), Array(9, 10, 11, 12), Array(13, 14, 15, 16))
    val order = "clockwise" // "anti-clockwise"
    val rotatedArray = rotateMatrix.rotate90(arr, order)
    rotateMatrix.printMatrix(rotatedArray)
  }
}
