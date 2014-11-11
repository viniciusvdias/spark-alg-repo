/* fpgrowth.scala */
package fptree

import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import org.apache.spark.Partitioner

import scala.collection.mutable.Map
import scala.collection.immutable.Stack
import scala.collection.immutable.Queue

import fptree._


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

    // build local fp-tree based on a subset of transactions
    def mkLocalTrees(transIter: Iterator[Array[Int]]): Iterator[FPTree] = {
      val tree = FPTree(Node.emptyNode, frequencyBcast.value,
        supBcast.value, miBcast.value, rhoBcast.value)
      tree.buildTree(transIter)
      Iterator(tree)
    }

    //println("Transactions")
    //transactionsRDD.foreach {trans => println(trans.mkString(", "))}
    //println()

    // here we have local trees
    val localTreesRDD = transactionsRDD.mapPartitions (mkLocalTrees)

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
    partitionBy(TreePartitioner(numberOfPartitions))
    
    // build a partition of the global fp-tree, given a set of miTrees
    def mkFpTree(chunksIter: Iterator[(Stack[Int],Node)]): Iterator[FPTree] = {
      val tree = FPTree(Node.emptyNode, null,
        supBcast.value, miBcast.value, rhoBcast.value)
      tree.buildTreeFromChunks(chunksIter)
      Iterator(tree)
    }

    val fpTreesRDD = miTreesRDD.mapPartitions (mkFpTree)
    //println("fpTreesRDD count = " + fpTreesRDD.count)

    // tests
    
    //localTreesRDD.foreach {tree => println("\n\n" + tree)}
    //fpTreesRDD.foreach {tree => println("\n\n" + tree)}
    
    val rhoTreesRDD = fpTreesRDD.flatMap (_.rhoTrees).
    partitionBy(TreePartitioner(numberOfPartitions))
    
    println("rhoTreesRDD count = " + rhoTreesRDD.count + " partitioner = " +
      rhoTreesRDD.partitioner)
    rhoTreesRDD.foreach {case (prefix, tree) =>
      println("prefix = " + prefix + "\n" + tree + "\n")
    }

    def mkCfpTree(prefixChunks: (Stack[Int],Iterable[Node])): FPTree = {
      val tree = FPTree(Node.emptyNode, null,
        supBcast.value, miBcast.value, rhoBcast.value)
      tree.buildCfpTreesFromChunks(prefixChunks)
      tree
    }
    def reduceCfpTree(t1: FPTree, t2: FPTree): FPTree = {
      t1.buildCfpTreesFromChunks((t1.itemSet, Iterable(t2.root)))
      t1
    }

    // +++++++++++++ version using reduceByKey (reduce-like)
    val finalFpTreesRDD = rhoTreesRDD.map {case (prefix, node) =>
      ( prefix, mkCfpTree((prefix, Iterable(node))) )
    }.
    reduceByKey(reduceCfpTree).
    map(_._2)

    println("finalFpTreesRDD count = " + finalFpTreesRDD.count)
    finalFpTreesRDD.foreach {tree => println("\n" + tree)}

    val itemSetsRDD = finalFpTreesRDD.map (_.fpGrowth()).
    flatMap (itemSet => itemSet).
    map {case (it, count) => (it.sorted.mkString(" "), count / nTransBcast.value.toDouble)}.
    sortByKey()

    println("\nItemSets ::: " + itemSetsRDD.count)
    itemSetsRDD.foreach {
      case (it, perc) =>
        println(it + "\t" + "%.6f".format(perc))
    }

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

