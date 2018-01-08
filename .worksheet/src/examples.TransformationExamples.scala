package examples

import common.utilities.SparkCommonUtils

object TransformationExamples {;import org.scalaide.worksheet.runtime.library.WorksheetSupport._; def main(args: Array[String])=$execute{;$skip(128); 
 val sc = SparkCommonUtils.spContext;System.out.println("""sc  : org.apache.spark.SparkContext = """ + $show(sc ));$skip(40); 
 
 val rdd1 = sc.parallelize(1 to 3, 1);System.out.println("""rdd1  : org.apache.spark.rdd.RDD[Int] = """ + $show(rdd1 ));$skip(38); 
 val rdd2 = sc.parallelize(4 to 6 ,1);System.out.println("""rdd2  : org.apache.spark.rdd.RDD[Int] = """ + $show(rdd2 ));$skip(31); val res$0 = 
 //union
 (rdd1++rdd2).collect;System.out.println("""res0: Array[Int] = """ + $show(res$0))}




}
