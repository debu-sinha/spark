package examples

import common.utilities.SparkCommonUtils
import java.time.Year

case class Stocks(date: String, symbol: String, open: Double, close: Double, low: Double, high: Double, volume: Double)

object StockAnalysis extends App{
  val sc = SparkCommonUtils.spContext
  val ss = SparkCommonUtils.spSession
  
  import ss.implicits._
  import org.apache.spark.sql.functions._
  import org.apache.spark.sql.types._
  import org.apache.spark.sql._
  
  val stocks = sc.textFile("/resources/prices-split-adjusted.csv",2)
  
  val header = stocks.first()
  
  val stocksDf = stocks.filter(row=>row!=header).map(r=>r.split(",")).map(x=>Stocks(x(0).toString, x(1).toString, x(2).toDouble, x(3).toDouble, x(4).toDouble,x(5).toDouble,x(6).toDouble)).toDS.cache

  stocksDf.show(3)
  
  stocksDf.select(year($"date").alias("yr"), month($"date").alias("mo"), ($"close"-$"open").alias("movement"), $"symbol").groupBy("yr","mo","symbol").avg("movement").orderBy("yr","mo","symbol").show
}