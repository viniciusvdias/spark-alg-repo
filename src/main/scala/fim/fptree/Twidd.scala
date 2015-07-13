/* Twidd.scala */
package fim.fptree

// command line parsing
import fim.util.ParamsParser

// common
import fim.util.Common

import org.apache.log4j.{Logger, Level}

// spark
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.{Partitioner, HashPartitioner, RangePartitioner}
import org.apache.spark.SparkContext._
import org.apache.spark.AccumulatorParam
import org.apache.spark.storage.StorageLevel
import org.apache.spark.rdd.RDD
import org.apache.spark.broadcast.Broadcast

// scala stuff
import scala.collection.mutable.ArrayBuffer
import scala.collection.mutable.WrappedArray
import scala.collection.mutable.Map
import scala.util.Sorting
import scala.math.Ordering

import scala.reflect._
import org.apache.spark.util.SizeEstimator
object Twidd {
  val appName = "FP-Twidd"
  val log = Logger.getLogger(appName)

  type ItemSet = WrappedArray[Int]

  case class Params(inputFile: String = null,
      minSupport: Double = 0.5,
      numPartitions: Int = 128,
      mu: Int = 2,
      rho: Int = 2,
      sep: String = "\\s+",
      logLevel: Level = Level.OFF) {

    def getValues = (inputFile, minSupport, numPartitions,
      mu, rho, sep, logLevel)

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

    parser.opt ("minSupport",
      s"path to transactions input file, default: ${defaultParams.minSupport}",
      (value,tmpParams) => tmpParams.copy (minSupport = value.toDouble)
      )

    parser.opt ("numPartitions",
      s"number of partitions, default: ${defaultParams.numPartitions}",
      (value,tmpParams) => tmpParams.copy (numPartitions = value.toInt)
      )

    parser.opt ("mu",
      s"mu load balacing factor, default: ${defaultParams.mu}",
      (value,tmpParams) => tmpParams.copy (mu = value.toInt)
      )

    parser.opt ("rho",
      s"rho pre-projection factor, default: ${defaultParams.rho}",
      (value,tmpParams) => tmpParams.copy (rho = value.toInt)
      )

    parser.opt ("separator",
      s"item separator, default: ${defaultParams.sep}",
      (value,tmpParams) => tmpParams.copy (sep = value)
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

  /*
   * create transactions RDD from raw lines.
   */
  def getTransactions (linesRDD: RDD[String], sep: String) = {
    
    val transactionsRDD = linesRDD
      .map {l =>
        (l split sep).flatMap {str =>
          // ignore non-32-Integers
          try Iterator(str.toInt)
          catch {
            case e: java.lang.NumberFormatException =>
              Iterator.empty
          }
        }
      }
      .persist (StorageLevel.MEMORY_ONLY_SER)
      .setName ("transactions_rdd")

    transactionsRDD
  }

  /*
   * each RDD partition turns into a local tree.
   */
  def localTrees(
    transactionsRDD: RDD[Array[Int]],
    freqsBc: Broadcast[scala.collection.Map[Int,Int]]) = {

    // building *numPartitions* local trees
    val localTreesRDD = transactionsRDD
      .map (_.filter(freqsBc.value.contains(_))
        .sortWith {(it1, it2) =>
          val cmp = (freqsBc.value(it1) compare freqsBc.value(it2))
          if (cmp == 0) it1 < it2
          else cmp > 0
        }
      )
      .mapPartitions {transIter =>
        val tree = FPTree()
        tree.buildTree(transIter)
        Iterator(tree)
      }
    localTreesRDD
  }

  /*
   * extract mu-trees (prefix,subtree) from local trees.
   */
  def muTrees(
      localTreesRDD: RDD[FPTree],
      mu: Int) = {

    val muTreesRDD = localTreesRDD.flatMap (FPTree.muTrees(_, mu))

    muTreesRDD
  }

  /*
   * shuffle mu-trees and merge them into balanced fp trees.
   */
  def mergeMuTrees(
      muTreesRDD: RDD[(ItemSet,Any)],
      partitioner: Partitioner) = {

    val fpTreesRDD = muTreesRDD.partitionBy (partitioner)
      .mapPartitions ({muTreesIter =>
        val tree = FPTree()
        while (!muTreesIter.isEmpty) {
          val (p,t) = muTreesIter.next
          tree.mergeMuTree(p, t)
        }
        Iterator(tree)
      }, true)

    fpTreesRDD
  }

  /*
   * pre-projection based on rho factor.
   */
  def rhoTrees(
      fpTreesRDD: RDD[FPTree],
      rho: Int) = {

    val rhoTreesRDD = fpTreesRDD.flatMap {FPTree.rhoTrees(_,rho)}
    rhoTreesRDD

  }

  /*
   * shuffle pre-projected rho-trees and merge them into final fp trees.
   */
  def mergeRhoTrees(
      rhoTreesRDD: RDD[(ItemSet,Any)],
      partitioner: Partitioner) = {

    val finalFpTreesRDD = rhoTreesRDD.reduceByKey (partitioner,
      (k,v) => (k,v) match {
        case (t1:Int,t2:Int) => t1 + t2
        case (t1:FPTree,t2:FPTree) => t1.mergeRhoTree(t2); t1
      }
    )

    finalFpTreesRDD
  }

  /*
   * finish projection by running fpGrowth on each final tree.
   */
  def runFPGrowth(
      finalFpTreesRDD: RDD[(ItemSet,Any)],
      minCount: Int) = {

    val itemSetsRDD = finalFpTreesRDD.flatMap {
      case (p,sup:Int) =>
        if (sup > minCount) Iterator( (p.toArray, sup) ) else Iterator.empty
      case (p,t:FPTree) =>
        t.itemSet = p.toArray; FPTree.fpGrowth(t, minCount)
    }

    itemSetsRDD
  }

  /* 
   * custom partitioner: it guarantees that subtrees with the same prefix are
   * grouped together.
   */
  case class TreePartitioner(numPartitions: Int) extends Partitioner {

    def getPartition(key: Any): Int = {
      key.hashCode % numPartitions match {
        case neg if neg < 0 => neg + numPartitions
        case pos => pos
      }
    }

    override def equals(other: Any): Boolean = other.isInstanceOf[TreePartitioner]
  }

  def run(params: Params) {

    // params as vals (lazy evaluation safety)
    val (inputFile, minSupport, numPartitions,
      mu, rho, sep, logLevel) = params.getValues

    log.setLevel(logLevel)
    Logger.getLogger("org").setLevel(Level.INFO)
    Logger.getLogger("akka").setLevel(Level.INFO)
    
    log.debug (s"\n\n${params}\n")

    val conf = new SparkConf().setAppName(appName)
    conf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    conf.set("spark.kryo.registrator", "fim.fptree.TreeOptRegistrator")
    //conf.set("spark.network.timeout", "360") // replaces all network-timeout configs
    //conf.set("spark.akka.timeout", "300")
    //conf.set("spark.core.connection.ack.wait.timeout", "100")
    //conf.set("spark.driver.maxResultSize", "0")
    //conf.set("spark.rdd.compress", "true")
    //conf.set("spark.storage.memoryFraction", "0.45")

    val sc = new SparkContext(conf)
    log.debug (s"spplicationId = ${sc.applicationId}")

    // Start execution time
    var t0 = System.nanoTime

    // frequency counting
    val linesRDD = sc.textFile(inputFile, numPartitions)
    
    // collection of transactions
    val transactionsRDD = getTransactions (linesRDD, sep)
    
    val transCount = transactionsRDD.count
    log.debug (s"number of transactions = ${transCount}")
    log.debug (Common.storageInfo (transactionsRDD))

    // minSupport percentage to minCount
    val minCount = (minSupport * transCount).toInt
    log.debug (s"support count = ${minCount} (${minSupport * 100}%)")

    // count 1-itemsets
    val frequencyRDD = transactionsRDD
      .flatMap (trans => trans.iterator zip Iterator.continually(1))
      .reduceByKey (_ + _)
      .filter {case (_,sup) => sup > minCount}

    // broadcast variables
    val freqs = frequencyRDD.collectAsMap
    val freqsBc = sc.broadcast(freqs)
    //transactionsRDD.unpersist()

    log.debug (s"${freqsBc.value.size} frequent 1-itemsets ...")
    
    // construct localTrees
    val localTreesRDD = localTrees (transactionsRDD, freqsBc)

    //sc.runJob(localTreesRDD, (iter: Iterator[_]) => {})
    //log.debug ("local trees: " + (System.nanoTime - t0) / (1000000000.0))
    //t0 = System.nanoTime

    // muTrees are constructed based on the custom partitioner 'TreePartitioner'
    val muTreesRDD = muTrees (localTreesRDD, mu)

    // partitioner
    val partitioner = TreePartitioner (numPartitions)
    //val partitioner = new RangePartitioner (numPartitions, muTreesRDD)(Ordering.by(_.hashCode), classTag[WrappedArray[Int]])

    // merge muTrees based on prefix
    val fpTreesRDD = mergeMuTrees (muTreesRDD, partitioner)
    
    //sc.runJob(fpTreesRDD, (iter: Iterator[_]) => {})
    //log.debug ("fp trees: " + (System.nanoTime - t0) / (1000000000.0))
    //t0 = System.nanoTime

    // build first projection trees and final conditional trees, respectively
    val rhoTreesRDD = rhoTrees (fpTreesRDD, rho)


    val finalFpTreesRDD = mergeRhoTrees (rhoTreesRDD, partitioner)
    
    //sc.runJob(finalFpTreesRDD, (iter: Iterator[_]) => {})
    //log.debug ("final trees: " + (System.nanoTime - t0) / (1000000000.0))
    //t0 = System.nanoTime

    // perform remaining projections in the partitioned trees
    val itemSetsRDD = runFPGrowth (finalFpTreesRDD, minCount)

    logLevel match {
      case Level.OFF =>
        println ("\nItemsets ::: " + itemSetsRDD.count)
        itemSetsRDD.foreach {case (it,sup) => println(it.mkString(",") + "\t" + sup)}

      case _ =>
        val currTime = System.currentTimeMillis
        val sufix = "_%s_%s.fpgrowth".format(inputFile.split("/").last,numPartitions)
        itemSetsRDD.map {case (it,sup) => it.sorted.mkString(",") + " " + sup}
          .saveAsTextFile (currTime + "_%s_%s_%s".format(minSupport,mu,rho) + sufix)
    }
    // End job execution time
    log.debug ("All execution took " + (System.nanoTime - t0) / (1000000000.0) + " seconds")
    sc.stop()
  }

  
}
