/* fpgrowth.scala */
package fim.fptree

import org.apache.log4j.Logger
import org.apache.log4j.Level

import org.apache.spark.SparkContext._
import org.apache.spark.{Logging, SparkContext}
import org.apache.spark.SparkConf
import org.apache.spark.Partitioner
import org.apache.spark.HashPartitioner
import org.apache.spark.RangePartitioner
import org.apache.spark.rdd.RDD
import org.apache.spark.storage.StorageLevel

import scala.collection.mutable.ArrayBuffer
import scala.collection.mutable.WrappedArray
import scala.util.Sorting
import scala.math.Ordering

object fpgrowth {
  val log = Logger.getLogger("fpgrowth")
  log.setLevel(Level.DEBUG)

  // default input parameters
  var inputFile = ""
  var sep = " "
  var minSup = 1
  var mu = 2
  var rho = 1
  var numberOfPartitions = 128

  val conf = new SparkConf().setAppName("Twidd")

  def main(args: Array[String]) {
    
    // command line arguments
    var minPctSup = minSup.toDouble
    try {

      inputFile = args(0)
      minPctSup = args(1).toDouble
      sep = args(2)
      mu = args(3).toInt
      rho = args(4).toInt
      numberOfPartitions = args(5).toInt

      // supported serializers
      args(6) match {
        case "os" => // optimized serializer (memory-friendly)
          conf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
          conf.set("spark.kryo.registrator", "fim.fptree.TreeOptRegistrator")
        case "ds" => // default serializer (probably Java-serializer)
        case _ => throw new Exception()
      }

      args(7) match {
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

    // Start execution time
    var t0 = System.nanoTime

    // frequency counting
    val linesRDD = sc.textFile(inputFile, numberOfPartitions)

    // collection of transactions
    val sepBc = sc.broadcast(sep)
    val transactionsRDD = linesRDD
      .map (l => (l split sepBc.value).map(it => it.toInt))
    val transCount = transactionsRDD.count.toInt
    log.debug ("number of transactions = " + transCount)
    val nTransBc = sc.broadcast(transCount)

    // minSupport percentage to minSupport count
    minSup = (minPctSup * transCount).toInt
    log.debug ("support count = " + minSup)
    val minSupBc = sc.broadcast(minSup)
    val muBc = sc.broadcast(mu)
    val rhoBc = sc.broadcast(rho)

    // count 1-itemsets
    val frequencyRDD = transactionsRDD
      .flatMap(trans => trans.iterator zip Iterator.continually(1))
      .reduceByKey(_ + _)
      .filter {case (_,sup) => sup > minSupBc.value}
    
    // broadcast variables
    val freqsBc = sc.broadcast(frequencyRDD.collectAsMap)

    log.debug (freqsBc.value.size + " frequent 1-itemsets ...")
     
    // here we have *numberOfPartitions* local trees
    val localTreesRDD = transactionsRDD
      .map (_.filter(freqsBc.value.contains(_)).
        sortWith {(it1, it2) =>
          val cmp = (freqsBc.value(it1) compare freqsBc.value(it2))
          if (cmp == 0) it1 < it2
          else cmp > 0
        }
      )
      .mapPartitions ({transIter =>
        val tree = FPTree()
        tree.buildTree(transIter)
        Iterator(tree)
      }, true)

    //sc.runJob(localTreesRDD, (iter: Iterator[_]) => {})
    //log.debug ("local trees: " + (System.nanoTime - t0) / (1000000000.0))
    //t0 = System.nanoTime

    // release frequency table, up to this point, 1-itemsets are no longer
    // necessary
    //freqsBc.unpersist()

    // muTrees are constructed based on the custom partitioner 'TreePartitioner'
    val muTreesRDD = localTreesRDD.flatMap (_.muTrees(muBc.value))
      .partitionBy(TreePartitioner(numberOfPartitions))

    // merge muTrees based on prefix
    val fpTreesRDD = muTreesRDD.mapPartitions ({muTreesIter =>
      val tree = FPTree()
      while (!muTreesIter.isEmpty) {
        val (p,t) = muTreesIter.next
        tree.mergeMuTree(p, t)
      }
      Iterator(tree)
    }, true)
    
    //sc.runJob(fpTreesRDD, (iter: Iterator[_]) => {})
    //log.debug ("fp trees: " + (System.nanoTime - t0) / (1000000000.0))
    //t0 = System.nanoTime

    // build first projection trees and final conditional trees, respectively
    val rhoTreesRDD = fpTreesRDD.flatMap {_.rhoTrees(rhoBc.value)}
    val finalFpTreesRDD = rhoTreesRDD.reduceByKey {
      case (t1:Int,t2:Int) => t1 + t2
      case (t1:FPTree,t2:FPTree) => t1.mergeRhoTree(t2); t1
    }
    
    //sc.runJob(finalFpTreesRDD, (iter: Iterator[_]) => {})
    //log.debug ("final trees: " + (System.nanoTime - t0) / (1000000000.0))
    //t0 = System.nanoTime

    // perform remaining projections in the partitioned trees
    val itemSetsRDD = finalFpTreesRDD.flatMap {
      case (p,sup:Int) => if (sup > minSupBc.value) Iterator( (p.toArray, sup) ) else Iterator.empty
      case (p,t:FPTree) => t.itemSet = p.toArray; t.fpGrowth(minSupBc.value)
    }

    args(7) match {
      case "log" =>
        val currTime = System.currentTimeMillis
        val sufix = "_%s_%s.fpgrowth".format(inputFile.split("/").last,numberOfPartitions)
        itemSetsRDD.map {case (it,sup) => it.sorted.mkString(",") + " " + sup}
          .saveAsTextFile (currTime + "_%s_%s_%s".format(minPctSup,mu,rho) + sufix)

      case "nolog" =>
        log.debug ("\nItemsets ::: " + itemSetsRDD.count)
        itemSetsRDD.foreach {case (it,sup) => println(it.mkString(",") + "\t" + sup)}
    }
    // End job execution time
    log.debug ("All execution took " + (System.nanoTime - t0) / (1000000000.0) + " seconds")

  }

  /* partitioner that guarantees that equal prefixes goes to the same
   * partition
   */
  case class TreePartitioner(numPartitions: Int) extends Partitioner {

    def getPartition(key: Any): Int = {
      key.hashCode.abs % numPartitions
    }

    override def equals(other: Any): Boolean = other.isInstanceOf[TreePartitioner]
  }
 
  def printHelp = {
    println("Usage:\n$ ./bin/spark-submit --class fim.fptree.fpgrowth" +
      " <jar_file> <input_file> <%_min_support> <item_separator> <mu_parameter>" +
      " <rho_parameter> <num_partitions> ds|os nolog|log\n")
  }

}

