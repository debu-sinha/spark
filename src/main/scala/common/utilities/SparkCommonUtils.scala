package common.utilities

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.sql.SparkSession
 

object SparkCommonUtils {

  
  val datadir =  "/home/edureka/workspace/projects/spark/src/main/resources"
  
  val appName = "App-Debu"
  
  //val sparkMasterURL = "local[4]"
  val sparkMasterURL = "spark://localhost:7077"
  //val sparkMasterURL = "yarn"
  
  val tempDir = "/tmp/spark-warehouse"
  
  var spSession: SparkSession = null
  var spContext: SparkContext = null
  
  //Initialization. Runs when object is created
  
  {
    System.setProperty("hadoop.home.dir","/usr/lib/hadoop-2.8.1" )
    
    val conf = new SparkConf()
    .setAppName(appName)
    .setMaster(sparkMasterURL)
    .set("spark.executor.memory","2g")
    .set("spark.sql.shuffle.partitions","2")
    
    //get or create a spark context
    spContext=SparkContext.getOrCreate(conf)
    
    
    //create a new spark SQL session
    spSession = SparkSession
    .builder()
    .appName(appName)
    .master(sparkMasterURL)
    .config("spark.sql.warehouse.dir", tempDir)
    .getOrCreate()
    
  }
   
}