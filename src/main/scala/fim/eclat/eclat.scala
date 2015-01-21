package fim.eclat

import org.apache.log4j.Logger
import org.apache.log4j.Level

import org.apache.spark.{Logging, SparkContext}
import org.apache.spark.SparkConf

object eclat {

  Logger.getLogger("org").setLevel(Level.INFO)
  Logger.getLogger("akka").setLevel(Level.INFO)

  // default input parameters
  var inputFile = ""
  var sep = " "
  var sup = 1
  var numberOfPartitions = 128

  val conf = new SparkConf().setAppName("FP-Eclat")
  conf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
  conf.set("spark.kryo.registrator", "fim.serialization.DefaultRegistrator")
  //conf.set("spark.kryoserializer.buffer.mb", "512")
  conf.set("spark.core.connection.ack.wait.timeout","600")
  
  def main(args: Array[String]) {
    
    // command line arguments
    var _sup = sup.toDouble
    try {
      inputFile = args(0)
      _sup = args(1).toDouble
      sep = args(2)
      numberOfPartitions = args(3).toInt

    } catch {
      case e: Exception =>
        printHelp
        return
    }

    val sc = new SparkContext(conf)
  }

  def printHelp = {
    println("Usage:\n$ ./bin/spark-submit --class fim.eclat.eclat " +
      " <jar_file> <input_file> <min_support> <item_separator> <num_partitions>\n")
  }

}

