package trie

/**
  * Created by Shreyansh Gupta on 2019-04-13
  */
class TrieDataStructure {

  val alphabetSize = 26

  def insert(root: TrieNode, key: String): Unit = {
    val n = key.length
    var pChild = root
    var i = 0
    while(i < n) {
      val index = key.charAt(i) - 'a'
      if (pChild.child(index) == null) {
        pChild.child(index) = TrieNode()
      }
      pChild = pChild.child(index)
      i += 1
    }
    pChild.leaf = true
  }

  // A recursive function to print all possible valid
  // words present in array
  def searchWord(root: TrieNode, Hash: Array[Boolean], str: String): Unit = {
    // if we found word in trie / dictionary
    if (root.leaf) {
      println(str)
    }
    // traverse all child's of current root
    var K = 0
    while ( {
      K < alphabetSize
    }) {
      if (Hash(K) && root.child(K) != null) {
        // add current character
        val c = (K + 'a').toChar
        // Recursively search reaming character
        // of word in trie
        searchWord(root.child(K), Hash, str + c)
      }
      K += 1
    }
  }

  // Prints all words present in dictionary.

  def printAllWords(Arr: Array[Char], root: TrieNode, n: Int): Unit = {
    // create a 'has character' array that will store all
    // present character in Arr[]
    val Hash = new Array[Boolean](alphabetSize)
    var i = 0
    while ( {
      i < n
    }) {
      Hash(Arr(i) - 'a') = true
      i += 1
    }
    // temporary node
    val pChild = root
    // string to hold output words
    var str = ""
    // Traverse all matrix elements. There are only
    // 26 character possible in char array
    i = 0
    while ( {
      i < alphabetSize
    }) {
      // we start searching for word in dictionary
      // if we found a character which is child
      // of Trie root
      if (Hash(i) && pChild.child(i) != null) {
        str = str + (i + 'a').toChar
        searchWord(pChild.child(i), Hash, str)
        str = ""
      }
      i += 1
    }
  }
}

object TrieDataStructure {
  def main(args: Array[String]): Unit = {

    val trieDS = new TrieDataStructure
    // Let the given dictionary be following
    val dict = Array("go", "bat", "me", "eat", "goal", "boy", "run")

    // Root Node of Trie
    val root = TrieNode()

    // insert all words of dictionary into trie
    val n = dict.length
    var i = 0
    while ( {
      i < n
    }) {
      trieDS.insert(root, dict(i))
      i += 1
    }

    val arr = Array('e', 'o', 'b', 'a', 'm', 'g', 'l')
    val N = arr.length

    trieDS.printAllWords(arr, root, N)
  }
}
