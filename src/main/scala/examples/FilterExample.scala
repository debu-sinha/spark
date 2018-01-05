package examples

import common.utilities.SparkCommonUtils


object FilterExample extends App{
  val sc = SparkCommonUtils.spContext
  
  val a =sc.parallelize(1 to 100, 3)
  
  val b =a.filter(_%2==0)
  
  b.collect().foreach(println)
}