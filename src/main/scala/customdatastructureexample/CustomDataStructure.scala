package customdatastructureexample

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer
import scala.util.Random

/**
  * Created by Shreyansh Gupta on 2019-04-11
  */
class CustomDataStructure {

  var arr: mutable.ArrayBuffer[Int] = mutable.ArrayBuffer.empty[Int]
  var hash: mutable.HashMap[Int, Int] = mutable.HashMap.empty[Int, Int]

  // A Theta(1) function to add an element to MyDS
  // data structure
  def add(x: Int): Unit = {
    // If element is already present, then noting to do
    if (!hash.contains(x)) {
      val s = arr.size
      // put element at the end of arr[]
      arr += x
      // And put in hash also
      hash += (x -> s) // hash.put(x, s)
    }
  }
  def remove(x: Int): Unit = {
    if (hash.contains(x)) {
      val index = hash(x)
      hash.remove(x)
      val arrSize = arr.size
      val last = arr.last
      swapEle(arr, index, arrSize - 1)
      // Remove last element (This is O(1))
      arr.remove(arrSize - 1)
      // Update hash table for new index of last element
      hash.put(last, index)
    }
  }

  def getRandom: Int = {
    // Find a random index from 0 to size - 1
    val randNum = Random.nextInt(arr.size)
    // Return element at randomly picked index
    arr(randNum)
  }

  def swapEle(arr: ArrayBuffer[Int], index1: Int, index2: Int): Unit = {
    val temp = arr(index1)
    arr(index1) = arr(index2)
    arr(index2) = temp
  }

  def search(x: Int): Int = {
    // Return the index of x from hash
    hash(x)
  }

  def printArr(): Unit = {
    arr.foreach(println)
  }
}

object CustomDataStructure {
  def main(args: Array[String]): Unit = {

    val myDS = new CustomDataStructure
    myDS.add(10)
    myDS.add(20)
    myDS.add(30)
    myDS.add(40)

    println(myDS.search(30))

    myDS.remove(20)
    myDS.add(50)

    println(myDS.search(50))

    println(myDS.getRandom)
  }
}
