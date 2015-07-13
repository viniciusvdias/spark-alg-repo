package fim.sampler

// command line parsing
import fim.util.ParamsParser

// utils
import fim.util.Common

import org.apache.log4j.{Logger, Level}

// spark
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.SparkContext._
import org.apache.spark.storage.StorageLevel
import org.apache.spark.rdd.RDD

object Sampler {
  val appName = "Sampler"
  val log = Logger.getLogger(appName)

  case class Params(inputFile: String = null,
      fraction: Double = 0.1,
      seed: Long = 1,
      logLevel: Level = Level.OFF) {

    def getValues = (inputFile, fraction, seed, logLevel)

    override def toString =
      "&%@ params " + getValues.productIterator.mkString (" ")
  }

  def main(args: Array[String]) {
  
    val defaultParams = Params()

    val parser = new ParamsParser[Params](appName)
    parser.opt ("inputFile",
      s"path to transactions input file, default: ${defaultParams.inputFile}",
      (value,tmpParams) => tmpParams.copy (inputFile = value),
      true
      )

    parser.opt ("fraction",
      s"fraction of data, default: ${defaultParams.fraction}",
      (value,tmpParams) => tmpParams.copy (fraction = value.toDouble)
      )

    parser.opt ("seed",
      s"random number generator seed, default: ${defaultParams.seed}",
      (value,tmpParams) => tmpParams.copy (seed = value.toLong)
      )

    parser.opt ("logLevel",
      s"log4j level, default: ${defaultParams.logLevel}",
      (value,tmpParams) => tmpParams.copy (logLevel = Level.toLevel(value))
      )
    
    parser.parse (args, defaultParams) match {
      case Some(params) => run(params)
      case None => sys.exit(1)
    }
  }

  def run(params: Params) {

    val (inputFile, fraction, seed, logLevel) = params.getValues

    // set log levels
    log.setLevel(logLevel)
    Logger.getLogger("org").setLevel(Level.INFO)
    Logger.getLogger("akka").setLevel(Level.INFO)

    val conf = new SparkConf().setAppName(appName)
    conf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    conf.set("spark.kryo.registrator", "fim.fptree.TreeOptRegistrator")

    val sc = new SparkContext(conf)
    log.debug (s"applicationId = ${sc.applicationId}")
    
    val linesRDD = sc.textFile(inputFile)
    val sampleRDD = Common.genDatabaseFraction (linesRDD, fraction, seed)

    sampleRDD.saveAsTextFile (s"${inputFile}-${fraction}-${seed}.sample")

    sc.stop()

  }
}
