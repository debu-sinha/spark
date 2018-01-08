package examples

import common.utilities.SparkCommonUtils

object SparkLineageExample extends App{
  val sc = SparkCommonUtils.spContext
  
  val r00 = sc.parallelize(0 to 9)
  
  val r01 = sc.parallelize(0 to 90 by 10)
  
  val r10 = r00 cartesian r01
 
  val r11 = r00.map(n=>(n,n))
  
  val r12 = r00 zip r01
  
  val r13 = r01.keyBy(_/20)
  
  val r20 = Seq(r11, r12, r13).foldLeft(r10)(_ union _)
}