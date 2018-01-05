package examples

import common.utilities.SparkCommonUtils

object CountByKeyExample extends App{
  val sc = SparkCommonUtils.spContext
  
  val a =sc.parallelize(List((1,"India"),(1,"India"),(2,"Canada"),(3,"China"),(4, "US")), 1)
  
  a.countByKey().foreach(println)
}