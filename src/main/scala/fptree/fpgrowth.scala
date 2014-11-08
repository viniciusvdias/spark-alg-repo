/* fpgrowth.scala */
package fptree

import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import org.apache.spark.Partitioner

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

    // item counting
    val (transactionsRDD,frequencyRDD) = frequencyCounting
    val transCount = transactionsRDD.count.toInt

    // broadcast variables
    val frequencyBcast = sc.broadcast(frequencyRDD.collect.toMap)
    val supBcast = sc.broadcast(sup)
    val miBcast = sc.broadcast(mi)
    val rhoBcast = sc.broadcast(rho)

    // build local fp-tree based on a subset of transactions
    def mkLocalTrees(transIter: Iterator[Array[String]]): Iterator[FPTree] = {
      val tree = FPTree(Node.emptyNode, frequencyBcast.value,
        supBcast.value, miBcast.value, rhoBcast.value)
      tree.buildTree(transIter)
      Iterator(tree)
    }

    println("Transactions")
    transactionsRDD.foreach {trans => println(trans.mkString(", "))}
    println()

    // here we have local trees
    val localTreesRDD = transactionsRDD.mapPartitions (mkLocalTrees)
    localTreesRDD.persist

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
    def mkFpTree(chunksIter: Iterator[(Stack[String],Node)]): Iterator[FPTree] = {
      val tree = FPTree(Node.emptyNode, null,
        supBcast.value, miBcast.value, rhoBcast.value)
      tree.buildTreeFromChunks(chunksIter)
      Iterator(tree)
    }

    val fpTreesRDD = miTreesRDD.mapPartitions (mkFpTree).
    persist
    println("fpTreesRDD count = " + fpTreesRDD.count)

    // tests
    
    //localTreesRDD.foreach {tree => println("\n\n" + tree)}
    fpTreesRDD.foreach {tree => println("\n\n" + tree)}
    
    val rhoTreesRDD = fpTreesRDD.flatMap (_.rhoTrees).
    partitionBy(TreePartitioner(numberOfPartitions)).
    persist
    
    println("rhoTreesRDD count = " + rhoTreesRDD.count + " partitioner = " +
      rhoTreesRDD.partitioner)
    rhoTreesRDD.foreach {case (prefix, tree) =>
      println("prefix = " + prefix + "\n" + tree + "\n")
    }

    def mkCfpTree(prefixChunks: (Stack[String],Iterable[Node])): FPTree = {
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
    map(_._2).
    persist

    println("finalFpTreesRDD count = " + finalFpTreesRDD.count)
    finalFpTreesRDD.foreach {tree => println("\n" + tree)}

    // ++++++++++++++ version using reduceByKey (barrier-like)
    //val finalFpTreesRDD = rhoTreesRDD.groupByKey.map (mkCfpTree).
    //persist
    //println("finalFpTreesRDD count = " + finalFpTreesRDD.count)
    //finalFpTreesRDD.foreach {tree => println("\n" + tree)}

    
  }

  def fpGrowth(
      tree: FPTree,
      freq: scala.collection.immutable.Map[String,Int],
      minSup: Int,
      mi: Int) = {

    // FP-Growth
    def fpGrowthRec(
        tree: FPTree,
        freq: scala.collection.immutable.Map[String,Int],
        prefix: Queue[String],
        minSup: Int,
        mi: Int,
        rho: Int,
        table: Map[String,Node]): List[(Queue[String],Int)] = tree.root.singlePath match {

      case p :: path => 
        val powerSet = fpgrowth.powerNoEmptySet[Node](path)
        val newPatterns = powerSet.map(s =>
            (
              //prefix.enqueue(
              //  fpgrowth.
              //  sortTransactionByFrequency(s.map(n => n.itemId), table, minSup).
              //  reduceLeft(_ + _)
              //  )
              prefix.enqueue(s.map(node => node.itemId)),
              s.map(node => (node.count, node.itemId)).min._1
            )
          )
        newPatterns

      case _ => 
        val sortedFList = table.map(e => e._2).toList.
        filter(_.count >= minSup).
        sortWith((a,b) => (a.count compare b.count) < 0)

        var newPatterns = List[(Queue[String],Int)]()

        // sortedFList has Nodes
        sortedFList.foreach {it =>
          var newTransactions = List[(Array[String],Int)]()
          println("Projecting over " + (prefix.enqueue(it.itemId)) )

          val newPatt = (prefix.enqueue(it.itemId), it.count)
          newPatterns = List(newPatt) ::: newPatterns
          var branch = it

          while (branch != null) {
            def makePath(curr: Node): Array[String] = {
              if (curr.isRoot) Array[String]()
              else makePath(curr.parent) ++ Array(curr.itemId)
            }
            val trans = makePath(branch)
            val count = branch.count
            newTransactions = List((trans.take(trans.size - 1), count)) ::: newTransactions
            branch = branch.link
          }

          println("New transactions")
          newTransactions.foreach {trans =>
            println(trans._1.foldLeft("")(_ + _))
          }
          
          val condTree = FPTree(Node.emptyNode, freq, minSup, mi, rho)
          val condTable = condTree.
          buildTree(newTransactions.flatMap {case (trans, count) =>
            (1 to count).map(_ => trans)
          }.iterator)
          
          println("New Table")
          println(condTable)
          println("New Tree")
          println(condTree)
          
          if (condTree.root.children.size > 0) {               
            newPatterns ++= fpGrowthRec(condTree, freq, newPatt._1, minSup, mi,
              rho, condTable)
          }
        }
        println("new pattern = " + newPatterns)
        newPatterns
    }
    
    fpGrowthRec(tree, freq, Queue[String](), minSup, mi, rho, tree.table)
  }
  
  /** Itemset counting, it returns a RDD composed by (<item>, <frequency>)-like
    * items.
    */
  def frequencyCounting = {
    val linesRDD = sc.textFile(inputFile)

    val transactionsRDD = linesRDD.
    repartition(numberOfPartitions).
    map(l => l split sep).
    map(a => (a.head, a.tail)).
    sortByKey().
    map{case (k, v) => v}

    val frequencyRDD = transactionsRDD.flatMap(trans => trans).
    map(it => (it, 1)).
    reduceByKey(_ + _)

    (transactionsRDD,frequencyRDD)
  }
  
  // power set, functional style
  def power[A](set: List[A]): List[List[A]] = {

    @annotation.tailrec
    def pwr(set: List[A], acc: List[List[A]]): List[List[A]] = set match {
      case Nil => acc
      case a :: as => pwr(as, acc ::: (acc map (a :: _)))
    }
    pwr(set, Nil :: Nil)
  }
  def powerNoEmptySet[A](set: List[A]): List[List[A]] = power(set).drop(1)

  def printHelp = {
    println("Usage:\n$ ./bin/spark-submit --class fpgrowth --master local" +
      " <jar_file> <input_file> <min_support> <item_separator>\n")
  }

}

