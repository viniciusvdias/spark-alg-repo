package dmining.fim.pfp

// log
import org.apache.log4j.Level
import org.apache.log4j.Logger

// spark
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.Partitioner
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext._
import org.apache.spark.storage.StorageLevel
import org.apache.spark.{Logging, SparkContext}
import org.apache.spark.rdd.RDD
import org.apache.spark.mllib.fpm.FPGrowth

// serializer
import com.esotericsoftware.kryo.Serializer
import org.apache.spark.serializer.KryoRegistrator
import com.esotericsoftware.kryo.Kryo

// scala stuff
import scala.annotation.tailrec
import scala.collection.mutable.Map
import scala.collection.mutable.ArrayBuffer

import scala.reflect.ClassTag

object pfp {

  type ItemSet = ArrayBuffer[Int]
  type TList = Array[Int]

  val log = Logger.getLogger("pfp-fpgrowth")
  log.setLevel(Level.DEBUG)

  // default input parameters
  var inputFile = ""
  var sep = " "
  var minSup = 1
  var numberOfPartitions = 128

  val conf = new SparkConf().setAppName("PFP")
  conf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
  
  def main(args: Array[String]) {
    
    // command line arguments
    var minPctSup = minSup.toDouble
    try {
      inputFile = args(0)
      minPctSup = args(1).toDouble
      sep = args(2)
      numberOfPartitions = args(3).toInt

      args(4) match {
        case "log" =>
        case "nolog" =>
          Logger.getLogger("org").setLevel(Level.OFF)
          Logger.getLogger("akka").setLevel(Level.OFF)
      }

    } catch {
      case e: Exception =>
        printHelp
        return
    }

    val sc = new SparkContext(conf)
    
    // start time
    var t0 = System.nanoTime
   
    // raw file input format and database partitioning
    val linesRDD = sc.textFile(inputFile, numberOfPartitions)
    //val sepBc = sc.broadcast(sep)

    val issuesAccum = sc.accumulator[Long] (0, "issues counter")

    // format transactions
    val transactionsRDD = linesRDD
      .map {l =>
        (l split "\\s+").flatMap {str =>
          try Iterator(str.toInt)
          catch {
            case e: java.lang.NumberFormatException =>
              issuesAccum += 1
              Iterator.empty
          }
        }
      }
      .persist (StorageLevel.MEMORY_ONLY)
      .setName ("transactions_rdd")
    
    // run the FP-growth algorithm
    val model = new FPGrowth()
      .setMinSupport(minPctSup)
      .setNumPartitions(numberOfPartitions)
      .run(transactionsRDD)
    
    args(4) match {
      case "log" =>
        val currTime = System.currentTimeMillis
        val sufix = "_%s_%s_%s.pfp"
          .format(inputFile.split("/").last,minPctSup,numberOfPartitions)

        model.freqItemsets
          //.map (itSet => itSet.items.mkString (",") + " " + itSet.freq)
          .saveAsTextFile (currTime + "_" + sufix)

      case "nolog" =>
        // print found frequent itemsets
        println(":: Itemsets " + model.freqItemsets.count)
        model.freqItemsets.foreach (itSet =>  itSet.items.mkString (",") + "\t" + itSet.freq)
    }
            
    log.debug (s"issues counter = ${issuesAccum.value}")
    log.debug ("All execution took " + (System.nanoTime - t0) / (1000000000.0) + " seconds")
    sc.stop()
  }

  def printHelp = {
    println("Usage:\n$ ./bin/spark-submit --class fim.pfp.fpgrowth " +
      " <jar_file> <input_file> <min_support> <item_separator> <num_partitions>\n")
  }

}

