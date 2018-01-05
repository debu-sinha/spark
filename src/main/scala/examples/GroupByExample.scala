package examples

import common.utilities.SparkCommonUtils

object GroupByExample extends App{
  val sc = SparkCommonUtils.spContext
  
  val a = sc.parallelize(1 to 100, 3)
  
  val b = a.groupBy(x => if(x%2==0) "even" else "odd")
  
  val c = b.collect().foreach(println)
}