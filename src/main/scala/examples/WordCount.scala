package examples

import common.utilities.SparkCommonUtils

object WordCount extends App{
  
  val spContext = SparkCommonUtils.spContext
  
  val file = spContext.textFile("<path to wordcount-sample.tsv>")
  
  file.flatMap(r => r.split("\t")).map(r=>(r,1)).reduceByKey((x,y)=>x+y).collect().foreach(println)
  
  
  
}
