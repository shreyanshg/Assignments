package trie

/**
  * Created by Shreyansh Gupta on 2019-04-13
  */

// trie Node
case class TrieNode(var leaf: Boolean, var child: Array[TrieNode])

object TrieNode{
  // Constructor
  def apply(): TrieNode = {
    val alphabetSize = 26
    val child = new Array[TrieNode](alphabetSize)
    // leaf is true if the node represents
    // end of a word
    val leaf = false

    var i = 0
    while ( {
      i < alphabetSize
    }) {
      child(i) = null
      i += 1
    }
    new TrieNode(leaf, child)
  }
}