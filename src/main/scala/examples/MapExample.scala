package examples

import common.utilities.SparkCommonUtils

object MapExample extends App{
  
  val animals = List("cow","dog", "cat", "goat", "chicken", "tiger", "panda","elephant","dragon")
  val rdd1= SparkCommonUtils.spContext.parallelize(animals, 2)
  
  val rdd2=rdd1.map(r=>r.length())
  
  val rdd3 = rdd1.zip(rdd2)
  
  rdd3.collect().foreach(println)
  
}