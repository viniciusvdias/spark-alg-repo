/* fpgrowth.scala */
package fim.fptree

import org.apache.log4j.Logger
import org.apache.log4j.Level

import org.apache.spark.SparkContext._
import org.apache.spark.{Logging, SparkContext}
import org.apache.spark.SparkConf
import org.apache.spark.Partitioner

import scala.collection.Iterator
import scala.collection.mutable.Map
import scala.collection.mutable.ListBuffer
import scala.collection.immutable.Stack
import scala.collection.immutable.Queue

// config kryo serializer
import com.esotericsoftware.kryo.io.Input
import com.esotericsoftware.kryo.io.Output
import com.esotericsoftware.kryo.Serializer
import com.esotericsoftware.kryo.Kryo
import org.apache.spark.serializer.KryoRegistrator

object fpgrowth {

  Logger.getLogger("org").setLevel(Level.INFO)
  Logger.getLogger("akka").setLevel(Level.INFO)

  // default input parameters
  var inputFile = ""
  var sep = " "
  var sup = 1
  var mi = 2
  var rho = 1
  var numberOfPartitions = 128

  val conf = new SparkConf().setAppName("FP-Growth")
  conf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
  //conf.set("spark.kryoserializer.buffer.mb", "512")
  // conf.set("spark.core.connection.ack.wait.timeout","6000")
  //conf.set("spark.akka.frameSize","100")

  def main(args: Array[String]) {
    
    // command line arguments
    var _sup = sup.toDouble
    try {
      inputFile = args(0)
      _sup = args(1).toDouble
      sep = args(2)
      mi = args(3).toInt
      rho = args(4).toInt
      numberOfPartitions = args(5).toInt

      // supported serializers
      if ( args(6) == "ss" ) // serial serializer
        conf.set("spark.kryo.registrator", "fim.serialization.SerialRegistrator")
      else if ( args(6) == "ds" ) // default serializer
        conf.set("spark.kryo.registrator", "fim.serialization.DefaultRegistrator")
      else throw new Exception()
    } catch {
      case e: Exception =>
        printHelp
        return
    }

    val sc = new SparkContext(conf)

    // Start execution time
    var t0 = System.nanoTime

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
      sup = (_sup * transCount).toInt
    else
      sup = (-1 * _sup).toInt

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
    sc.runJob(localTreesRDD, (iter: Iterator[_]) => {})
    println( (System.nanoTime - t0) / (1000000000.0) )
    t0 = System.nanoTime

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
          tree.root.children.foreach { c => nodes.push( (Stack[Int](), c) ) }

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

            node.children.foreach {c => nodes.push( (newPrefix, c) )}
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
    sc.runJob(fpTreesRDD, (iter: Iterator[_]) => {})
    println( (System.nanoTime - t0) / (1000000000.0) )
    t0 = System.nanoTime

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
    
    sc.runJob(finalFpTreesRDD, (iter: Iterator[_]) => {})
    println( (System.nanoTime - t0) / (1000000000.0) )
    t0 = System.nanoTime

    //finalFpTreesRDD.foreach {case t =>
    //  println("\n" + t)
    //}
    
    val itemSetsRDD = finalFpTreesRDD.map (_.fpGrowth()).
    flatMap (itemSet => itemSet)

    
    //sc.parallelize(Array(System.nanoTime -
    //  t0)).saveAsTextFile("%s_%s_%s_%d_%s_metrics.out".format(inputFile,sup,mi,rho,numberOfPartitions))
    
    
    //val fancyItemSetsRDD = itemSetsRDD.
    //map {case (it, count) => (it.sorted.mkString(" "), count / nTransBcast.value.toDouble)}.
    //sortByKey()

    // End job execution time
    //itemSetsRDD.saveAsTextFile("%s_%s_%s_%s_%s.out".format(inputFile,_sup,mi,rho,numberOfPartitions))
    println( (System.nanoTime - t0) / (1000000000.0) )

    println("\nItemSets ::: " + itemSetsRDD.count)
    itemSetsRDD.foreach {
      case (it, perc) =>
        println(it + "\t" + perc)
    }

    // ++++++++++++++ version using groupByKey (barrier-like)
    //val finalFpTreesRDD = rhoTreesRDD.groupByKey.map (mkCfpTree)
    //println("finalFpTreesRDD count = " + finalFpTreesRDD.count)
    //finalFpTreesRDD.foreach {tree => println("\n" + tree)}
    //
  }
  
  def printHelp = {
    println("Usage:\n$ ./bin/spark-submit --class fptree.fpgrowth" +
      " <jar_file> <input_file> <min_support> <item_separator> <mu_parameter>" +
      " <rho_parameter> <num_partitions> <serialization_register>\n")
  }

}

