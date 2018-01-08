package examples

import common.utilities.SparkCommonUtils

object RddJoinExamples extends App{
    val sc= SparkCommonUtils.spContext
    
    val alphabets= List("A","B","C","D","E")
    val indexAlphabets = sc.parallelize(alphabets.zipWithIndex)
    
    val alphabets2 = List("A" ,"C", "X", "Y", "B", "Z", "V")
    val indexedAlphabets2 = sc.parallelize(alphabets2.zipWithIndex)
    
    //join example
    val join = indexAlphabets.join(indexedAlphabets2)
    join.saveAsTextFile("hdfs://master:9000/join.txt")
    
    
    //left outer join
    val leftOuterJoin = indexAlphabets.leftOuterJoin(indexedAlphabets2)
    leftOuterJoin.saveAsTextFile("hdfs://master:9000/leftOuterJoin.txt")
    
    //right outer join
     val rightOuterJoin = indexAlphabets.rightOuterJoin(indexedAlphabets2)
    rightOuterJoin.saveAsTextFile("hdfs://master:9000/rightOuterJoin.txt")
    
    //full outer join
     val fullOuterJoin = indexAlphabets.fullOuterJoin(indexedAlphabets2)
     fullOuterJoin.saveAsTextFile("hdfs://master:9000/fullOuterJoin.txt")
    
}