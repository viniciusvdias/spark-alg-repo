/* fpgrowth.scala */
package fptree

import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import org.apache.spark.Partitioner

import scala.collection.Iterator
import scala.collection.mutable.Map
import scala.collection.immutable.Stack
import scala.collection.immutable.Queue

object fpgrowth {

  // default input parameters
  var inputFile = ""
  var sep = " "
  var sup = 1
  var mi = 2
  var rho = 1
  var numberOfPartitions = 2

  val conf = new SparkConf().setAppName("FP-Growth")
  val sc = new SparkContext(conf)
  
  def main(args: Array[String]) {

    println("job started")

    // command line arguments
    try {
      inputFile = args(0)
      sup = args(1).toInt
      sep = args(2)
      mi = args(3).toInt
      rho = args(4).toInt
    } catch {
      case e: Exception =>
        printHelp
        return
    }

    // frequency counting
    val sepBcast = sc.broadcast(sep)
    val linesRDD = sc.textFile(inputFile)

    val transactionsRDD = linesRDD.
    map(l => (l split sepBcast.value).map(it => it.toInt))

    val frequencyRDD = transactionsRDD.flatMap(trans => trans).
    map(it => (it, 1)).
    reduceByKey(_ + _)
    
    val transCount = transactionsRDD.count.toInt

    // broadcast variables
    val frequencyBcast = sc.broadcast(frequencyRDD.collect.toMap)
    val supBcast = sc.broadcast(sup)
    val miBcast = sc.broadcast(mi)
    val rhoBcast = sc.broadcast(rho)
    val nTransBcast = sc.broadcast(transCount)

    // here we have local trees
    val localTreesRDD = transactionsRDD.mapPartitions {transIter =>
      val tree = FPTree(Node.emptyNode, frequencyBcast.value,
        supBcast.value, miBcast.value, rhoBcast.value)
      tree.buildTree(transIter)
      Iterator(tree)
    }

    frequencyBcast.unpersist()

    /* partitioner that guarantees that equal prefixes goes to the same
     * partition
     */
    case class TreePartitioner(numPartitions: Int) extends Partitioner {

      def getPartition(key: Any): Int =
        key.hashCode.abs % numPartitions

      override def equals(other: Any): Boolean =
        other.isInstanceOf[TreePartitioner]
    }

    // miTrees are constructed based on the custom partitioner 'TreePartitioner'
    val miTreesRDD = localTreesRDD.
    flatMap (_.miTrees).
    partitionBy(TreePartitioner(localTreesRDD.partitions.size))

    //miTreesRDD.foreach {case (p, t) =>
    //  println("prefix = " + p + "\n" + t)
    //}
    
    val fpTreesRDD = miTreesRDD.mapPartitions {chunksIter =>
      val tree = FPTree(Node.emptyNode, null,
        supBcast.value, miBcast.value, rhoBcast.value)
      tree.buildTreeFromChunks(chunksIter)
      Iterator(tree)
    }
    
    val rhoTreesRDD = fpTreesRDD.flatMap (_.rhoTrees).
    partitionBy(TreePartitioner(fpTreesRDD.partitions.size))
    
    // +++++++++++++ version using reduceByKey (reduce-like)
    val finalFpTreesRDD = rhoTreesRDD.map {case (prefix, node) =>
      val tree = FPTree(Node.emptyNode, null,
        supBcast.value, miBcast.value, rhoBcast.value)
      tree.buildCfpTreesFromChunks((prefix, Iterable(node)))
      ( prefix, tree)
    }.
    reduceByKey {(t1, t2) =>
      t1.buildCfpTreesFromChunks((t1.itemSet, Iterable(t2.root)))
      t1
    }.
    map(_._2)

    val itemSetsRDD = finalFpTreesRDD.map (_.fpGrowth()).
    flatMap (itemSet => itemSet).
    map {case (it, count) => (it.sorted.mkString(" "), count / nTransBcast.value.toDouble)}.
    sortByKey()

    itemSetsRDD.saveAsTextFile("fptree.out")

    //println("\nItemSets ::: " + itemSetsRDD.count)
    //itemSetsRDD.foreach {
    //  case (it, perc) =>
    //    println(it + "\t" + "%.6f".format(perc))
    //}

    // ++++++++++++++ version using groupByKey (barrier-like)
    //val finalFpTreesRDD = rhoTreesRDD.groupByKey.map (mkCfpTree)
    //println("finalFpTreesRDD count = " + finalFpTreesRDD.count)
    //finalFpTreesRDD.foreach {tree => println("\n" + tree)}
  }
  
  def printHelp = {
    println("Usage:\n$ ./bin/spark-submit --class fpgrowth --master local" +
      " <jar_file> <input_file> <min_support> <item_separator>\n")
  }

}

