/* fpgrowth.scala */
package fptree

import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import org.apache.spark.Partitioner

import scala.collection.Iterator
import scala.collection.mutable.Map
import scala.collection.mutable.ListBuffer
import scala.collection.immutable.Stack
import scala.collection.immutable.Queue


// config kryo serializer
import com.esotericsoftware.kryo.Kryo
import org.apache.spark.serializer.KryoRegistrator
class MyRegistrator extends KryoRegistrator {
  override def registerClasses(kryo: Kryo) {
    kryo.register(classOf[Node])
    kryo.register(classOf[FPTree])
  }
}

object fpgrowth {

  // default input parameters
  var inputFile = ""
  var sep = " "
  var sup = 1
  var mi = 2
  var rho = 1
  var numberOfPartitions = 128

  val conf = new SparkConf().setAppName("FP-Growth")
  conf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
  conf.set("spark.kryo.registrator", "fptree.MyRegistrator")
  conf.set("spark.kryoserializer.buffer.mb", "512")
  conf.set("spark.core.connection.ack.wait.timeout","600")
  val sc = new SparkContext(conf)
  
  def main(args: Array[String]) {

    println("job started")

    // command line arguments
    try {
      inputFile = args(0)
      sup = args(1).toDouble
      sep = args(2)
      mi = args(3).toInt
      rho = args(4).toInt
      numberOfPartitions = args(5).toInt
    } catch {
      case e: Exception =>
        printHelp
        return
    }

    // frequency counting
    val sepBcast = sc.broadcast(sep)
    val linesRDD = sc.textFile(inputFile)

    val transactionsRDD = linesRDD.
    map(l => (l split sepBcast.value).map(it => it.toInt)).
    repartition(numberOfPartitions)

    val frequencyRDD = transactionsRDD.flatMap(trans => trans).
    map(it => (it, 1)).
    reduceByKey(_ + _)
    
    val transCount = transactionsRDD.count.toInt
    if (sup > 0)
      sup = sup * transCount
    else
      sup = -1 * sup

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
    //flatMap (_.miTrees).
    flatMap {tree =>

        val miChunks = ListBuffer[(Stack[Int], Node)]()
        if (tree.mi <= 0)
          miChunks.append( (Stack(tree.root.itemId), tree.root) )
        else{
          
          val nodes = scala.collection.mutable.Stack[(Stack[Int], Node)]()
          tree.root.children.foreach {case (_,c) => nodes.push( (Stack[Int](), c) ) }

          while (!nodes.isEmpty) {
            val (prefix, node) = nodes.pop()
            
            val newPrefix = prefix :+ node.itemId

            if (node.level < tree.mi && node.tids > 0) {

              val emptyNode = Node(node.itemId, node.tids, null, node.tids)
              emptyNode.count = node.tids
              miChunks.append( (newPrefix, emptyNode) )

            } else if (node.level == tree.mi) {

              miChunks.append( (newPrefix, node) )

            }

            node.children.foreach {case (_,c) => nodes.push( (newPrefix, c) )}
          }
        }
        miChunks
    }.
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
    
    val rhoTreesRDD = fpTreesRDD.flatMap (_.rhoTrees)//.
    //partitionBy(TreePartitioner(fpTreesRDD.partitions.size))
    
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

