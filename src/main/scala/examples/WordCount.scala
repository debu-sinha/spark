package examples

import common.utilities.SparkCommonUtils
import org.apache.spark.SparkContext._ 

object WordCount extends App{
  
  val spContext = SparkCommonUtils.spContext
  
  val file = spContext.textFile("/home/edureka/workspace/projects/spark/src/main/resources/wordcount-sample.tsv")
  
  val wc = file.flatMap(r => r.split("\t")).map(r=>(r,1)).reduceByKey((x,y)=>x+y)
  
  wc.saveAsTextFile("hdfs://master:9000/eclipse-save-textfile")
  
  wc.saveAsSequenceFile("hdfs://master:9000/eclipse-seq-file")
  
}
