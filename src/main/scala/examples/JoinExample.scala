package examples

import common.utilities.SparkCommonUtils

object JoinExample extends App{
  val sc = SparkCommonUtils.spContext
  
  val a= sc.parallelize(List("dog", "cat", "salmon", "salmon", "rat","elephant"), 3)
  
  val b= a.keyBy(_.length())
  
  val c = sc.parallelize(List("dog", "cat", "gnu", "salmon", "rabbit","turkey", "wolf", "bear" ,"bee"), 3)
  
  val d= c.keyBy(_.length())
  
  val e = b.join(d)
  
  e.collect().foreach(println)
  
}