package examples

import common.utilities.SparkCommonUtils

object ReduceExample extends App{
  val sc= SparkCommonUtils.spContext
  
  val a = sc.parallelize(1 to 100, 2)
  
  //reduce action
  println(a.reduce(_+_))
  
}