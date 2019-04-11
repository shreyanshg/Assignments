package binarytree

case class Node(var data: String, var left: Node, var right: Node)

object Node{
  def apply(data: String): Node = {
    Node(data, null, null)
  }
}