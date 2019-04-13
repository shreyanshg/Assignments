package binarytree

class BinaryTree {
  var root: Node = _

  def printSpiral(node: Node): Unit = {
    val h = height(node)
    var i = 1

    // ltr -> left to right. If this variable is set then the
    // given label is traversed from left to right

    var ltr = false
    while ( {
      i <= h
    }) {
      printGivenLevel(node, i, ltr)
      // Revert ltr to traverse next level in opposite order
      ltr = !ltr
      i += 1
    }
  }

  def printGivenLevel(node: Node, level: Int, ltr: Boolean): Unit = {
//    if (node == null) {
//      Unit
//    }
    if (level == 1) {
      println(node.data + " ")
    } else if (level > 1) {
      if (ltr) {
        printGivenLevel(node.left, level - 1, ltr)
        printGivenLevel(node.right, level - 1, ltr)
      }
      else {
        printGivenLevel(node.right, level - 1, ltr)
        printGivenLevel(node.left, level - 1, ltr)
      }
    }
  }

  // Compute the "height" of a tree -- the number of
  // nodes along the longest path from the root node
  // down to the farthest leaf node
  def height(node: Node): Int = {
    if (node == null) {
      0
    }
    else {
      // compute the height of each subtree
      val lHeight = height(node.left)
      val rHeight = height(node.right)

      /* use the larger one */
      if (lHeight > rHeight) {
        lHeight + 1
      }
      else {
        rHeight + 1
      }
    }
  }

}
object BinaryTree{
  def main(args: Array[String]): Unit = {
    val tree = new BinaryTree
    tree.root = Node("1")
    tree.root.left = Node("2")
    tree.root.right = Node("3")
    tree.root.left.left = Node("7")
    tree.root.left.right = Node("6")
    tree.root.right.left = Node("5")
    tree.root.right.right = Node("4")
    tree.printSpiral(tree.root)
  }
}