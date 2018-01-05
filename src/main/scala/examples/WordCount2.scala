package examples

import common.utilities.SparkCommonUtils

object WordCount2 extends App{
  val spContext = SparkCommonUtils.spContext
  
  val file = spContext.textFile("<path to wordcount-sample2.txt>")
  
  file.map(r=>(r,1)).reduceByKey((x,y)=>x+y).collect().foreach(println)
  
  
}
