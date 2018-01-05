package examples

import common.utilities.SparkCommonUtils

object TakeOrderedExample extends App{
  val sc = SparkCommonUtils.spContext
  
  val a = sc.parallelize(List("dog", "cat", "ape", "salmon"), 2)
  
  //action
  a.takeOrdered(2).foreach(println)
}