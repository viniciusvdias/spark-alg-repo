package fim.eclat

import org.apache.log4j.Level
import org.apache.log4j.Logger

import org.apache.spark.AccumulatorParam
import org.apache.spark.Partitioner
import org.apache.spark.rdd.CombinationRDD
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext._
import org.apache.spark.storage.StorageLevel
import org.apache.spark.{Logging, SparkContext}


import org.apache.spark.rdd._
import scala.annotation.tailrec

import scala.collection.mutable.BitSet
import scala.collection.mutable.ListBuffer
import scala.collection.mutable.Map
import scala.collection.mutable.Stack
import scala.collection.mutable.TreeSet

import scala.reflect.ClassTag

object eclat4 {

  type ItemSet = (Vector[Int], Set[Int])

  //Logger.getLogger("org").setLevel(Level.OFF)
  //Logger.getLogger("akka").setLevel(Level.OFF)
  val log = Logger.getLogger("eclat")
  log.setLevel(Level.DEBUG)

  // default input parameters
  var inputFile = ""
  var sep = " "
  var sup = 1
  var numberOfPartitions = 128

  val conf = new SparkConf().setAppName("FP-Eclat")
  conf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
  conf.set("spark.kryo.registrator", "fim.serialization.DefaultRegistrator")
  //conf.set("spark.kryoserializer.buffer.mb", "128")
  //conf.set("spark.core.connection.ack.wait.timeout","600")
  
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
   
    // raw file input format and database partitioning
    val linesRDD = sc.textFile(inputFile, numberOfPartitions)
    val sepBc = sc.broadcast(sep)

    // format transactions
    var transactionsRDD = linesRDD
      .map(l => (l split sepBc.value).map(it => it.toInt).sortBy(-_))

    // get transaction's count to estimate minSupport count
    val transCount = transactionsRDD.count.toInt
    val tCountBc = sc.broadcast(transCount)
    log.debug("transaction count = " + transCount)

    // minimum support percentage to support count
    sup = (_sup * transCount).toInt
    val supBc = sc.broadcast(sup)
    log.debug("support count = " + sup)
    
    // 1-itemset counting and broadcast to first prunning
    val frequencyRDD = transactionsRDD
      .flatMap (t => t.map (Vector(_)) zip Stream.continually(1))
      .reduceByKey (_ + _)
      .filter {case (_,sup) => sup > supBc.value}
      //.persist (StorageLevel.MEMORY_ONLY_SER)
    
        
    var level = 1
    val itemSets = ListBuffer[RDD[(Vector[Int],Int)]](frequencyRDD)
    var nItens = itemSets.last.count
    var nFreqs = 0L

    while (nItens > 0) {

      nFreqs += nItens
    
      val freqsBc = sc.broadcast (itemSets.last.collectAsMap)
      log.debug (freqsBc.value.size + " frequent " + level + "-itemsets ..")

      if (level == 1)
        transactionsRDD = transactionsRDD
          .map (_.filter (it => freqsBc.value.contains(Vector(it))) )
          .persist(StorageLevel.MEMORY_AND_DISK_SER)

      itemSets.append(
        transactionsRDD.flatMap {t =>
          val candidates = ListBuffer[Vector[Int]]()

          def binCoeff(n: Int, k: Int, threshold: Int): Boolean = {
            var i = n
            var coeff = 1
            while (i > (n-k)) {
              coeff *= i
              if (coeff > threshold) return false
              i -= 1
            }
            true
            //(2 to k).foreach (j => coeff /= j)
            //coeff
          }

          val (prefixes,cond) =
            if (binCoeff(t.size, level, freqsBc.value.size))
              (t.toVector.combinations(level), (p: Vector[Int]) => freqsBc.value.contains(p))
            else
              (freqsBc.value.keysIterator, (p: Vector[Int]) => p.diff(t.toVector).isEmpty)

          for (p <- prefixes; if cond(p)) {
            var break = false
            val tIter = t.iterator
            while (!break && tIter.hasNext) {
              val it = tIter.next
              if (it > p.head) candidates.append( Vector(it) ++ p )
              else break = true
            }
          }
          
          candidates.iterator zip Iterator.continually(1)
        }
        .reduceByKey (_ + _)
        .filter {case (_,sup) => sup > supBc.value}
        //.persist (StorageLevel.MEMORY_ONLY_SER)
      )
    
      nItens = itemSets.last.count
      freqsBc.unpersist()

      level += 1
    }

    // print found frequent itemsets
    println(":: Itemsets " + nFreqs)
    itemSets.foreach (_.foreach {
      case (it,sup) => println(it.mkString(",") + "\t" + sup)}
    )
    //itemSetsAccum.value.foreach {case (it,sup) =>
    //  println(it.mkString(",") + "\t" + sup)
    //}
  }

  def splitPartitions[T:ClassTag](rdd: RDD[T]) = {
    rdd.partitions.map {partition =>
      val idx = partition.index
      rdd.mapPartitionsWithIndex (
        (id,iter) => if (id == idx) iter else Iterator(),
        true)
    }
  }

  // equivalence class string representation
  def eqClassAsString(eqClassRDD: RDD[ItemSet]) = {
    "$$ " +
    eqClassRDD.collect.map {case (it,tids) =>
      it.mkString(",") + "::" + tids.mkString(",")
    }.mkString("\t")
  }

  object Combinations4 {

    def combinations[T:ClassTag](rdd: RDD[T]): RDD[(T,T)] = {
      @tailrec
      def combs[T:ClassTag](rdd: RDD[T], count: Long, acc: RDD[(T,T)]):RDD[(T,T)] = {
        val sc = rdd.context
        if (count < 2) { 
          acc
        } else if (count == 2) {
          val values = rdd.collect
          acc.union( sc.makeRDD[(T,T)](Seq((values(0), values(1)))) )
        } else {
          val elem = rdd.take(1)
          val elemRdd = sc.makeRDD(elem)
          val subtracted = rdd.subtract(elemRdd)  
          val comb = subtracted.map(e  => (elem(0),e))
          combs(subtracted, count - 1, acc.union(comb))
        } 
      }
      val count = rdd.count
      combs(rdd, count, rdd.context.makeRDD[(T,T)](Seq.empty[(T,T)]))
    }

    
  }
  
  class SetAccumulatorParam4[T:ClassTag] extends AccumulatorParam[Set[T]] {
    def zero(initialValue: Set[T]): Set[T] = {
      initialValue
    }
    def addInPlace(v1: Set[T], v2: Set[T]): Set[T] = {
      v1.union(v2)
    }
  }

  case class PrefixPartitioner4(numPartitions: Int) extends Partitioner {

        def getPartition(key: Any): Int = {
      key.asInstanceOf[Vector[Int]].dropRight(1)
        .mkString("").hashCode.abs % numPartitions
    }

    override def equals(other: Any): Boolean = {
      other.isInstanceOf[PrefixPartitioner4]
    }
  }

  def printHelp = {
    println("Usage:\n$ ./bin/spark-submit --class fim.eclat.eclat " +
      " <jar_file> <input_file> <min_support> <item_separator> <num_partitions>\n")
  }

}

