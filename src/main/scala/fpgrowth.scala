/* fpgrowth.scala */

import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf

object fpgrowth {

  var inputFile = ""
  var sep = " "
  var minSup = 1

  val conf = new SparkConf().setAppName("FP-Growth")
  val sc = new SparkContext(conf)
  
  def main(args: Array[String]) {

    // command line arguments
    try {
      inputFile = args(0)
      minSup = args(1).toInt
      sep = args(2)
    } catch {
      case e: Exception =>
        printHelp
        return
    }

    // item counting
    val frequencyRDD = frequencyCounting
    val frequencyBcast = sc.broadcast(frequencyRDD.collect)

    // access broadcast variable
    frequencyBcast.value.foreach(println)


    //frequencyRDD.foreach(println)
 
  }
  /** Itemset counting, it returns a RDD composed by (<item>, <frequency>)-like
    * items.
    */
  def frequencyCounting = {
    val linesRDD = sc.textFile(inputFile)
    val transactionsRDD = linesRDD.map(l => l split sep)
    val frequencyRDD = transactionsRDD.flatMap(trans => trans)
      .map(it => (it, 1))
      .reduceByKey(_ + _)
    frequencyRDD
  }

  def printHelp = {
    println("Usage:\n$ ./bin/spark-submit --class fpgrowth --master local" +
      " <jar_file> <input_file> <min_support> <item_separator>\n")
  }

}

