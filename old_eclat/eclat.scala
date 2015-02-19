package fim.eclat

import org.apache.log4j.Logger
import org.apache.log4j.Level

import org.apache.spark.{Logging, SparkContext}
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import org.apache.spark.Partitioner
import org.apache.spark.AccumulatorParam
import org.apache.spark.storage.StorageLevel
import org.apache.spark.rdd.CombinationRDD


import org.apache.spark.rdd._
import scala.annotation.tailrec

import scala.collection.mutable.TreeSet
import scala.collection.mutable.ListBuffer
import scala.collection.mutable.BitSet
import scala.collection.mutable.Stack
import scala.collection.mutable.Map

import scala.reflect.ClassTag

object eclat {

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
    val transactionsRDD = linesRDD
      .map(l => (l split sepBc.value).map(it => it.toInt))

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
      .flatMap (t => t zip Stream.continually(1))
      .reduceByKey (_ + _)
      .filter {case (_,sup) => sup > supBc.value}
    val freqsBc = sc.broadcast (frequencyRDD.collectAsMap)

    // frequent itemsets accumulator in the driver, at first
    val tSetAccum = sc.accumulator(Set[Int]())(new SetAccumulatorParam())
    val itemSetsAccum = sc.accumulator(
      freqsBc.value.map {case (it,sup) => (Vector(it),sup)}.toSet
    )(new SetAccumulatorParam())

    // build tids for 1-itemsets
    val tidsRDD = transactionsRDD.zipWithUniqueId()
      .flatMap {case (t,tid) =>

        // only frequent single-items will perdure
        val ft = t.flatMap {it =>
          if (freqsBc.value.contains(it)) Iterator(Vector(it))
          else Iterator()
        }
        
        // in case of diffSets instead of tidSets, it accumulates transactions
        // ids in the driver
        tSetAccum.add(Set(tid.toInt))

        ft.iterator zip Iterator.continually(Set(tid.toInt))
      }
      .reduceByKey (_.union(_))

        
    // at this point, workers does not need frequency table anymore
    freqsBc.unpersist()
   
    // stack to recursively mine equivalence classes
    val eqClasses = Stack[(Int,RDD[(Vector[Int],Set[Int])])]( (1,tidsRDD) )

    // there's unprocessed equivalence classes
    while (!eqClasses.isEmpty) {

      val (level,eqClassRDD) = eqClasses.pop()
      eqClassRDD.persist(StorageLevel.MEMORY_AND_DISK_SER)

      eqClassRDD.count match {
        case 0 | 1 =>
        case nItens =>
          log.debug("tids count (level=" + level + ") = " + nItens)
          log.debug("accumulator count = " + tSetAccum.value.size)

          // print equivalence class
          //log.debug(eqClassAsString(eqClassRDD))

          // combine (k-1)-length items to get a new eqClass, having k-length
          // itemsets
          val keyFunc = (it: (Vector[Int],Set[Int])) => it._1.last
          val newEqClassRDD = new CombinationRDD(sc, eqClassRDD, keyFunc)
            .flatMap {case ((it1,tids1), (it2,tids2)) =>
              // filter infrequent k-itemsets and accumulate (k-1)-itemsets

              val intersect = tids1.intersect(tids2)
              if (intersect.size > supBc.value){

                val newItemSet = it1 ++ Vector(it2.last)
                itemSetsAccum.add( Set( (newItemSet, intersect.size) ) )
                Iterator( (newItemSet, intersect) )

              } else Iterator()

            }
            .partitionBy(PrefixPartitioner(nItens.toInt))

            // split k-itemsets into new eqClasses, push them to stack
            val rddList = splitPartitions (newEqClassRDD)
            eqClasses.pushAll( rddList.map ((level+1,_)) )
      }
      eqClassRDD.unpersist()
    }

    // print found frequent itemsets
    println(":: Itemsets " + itemSetsAccum.value.size)
    itemSetsAccum.value.foreach {case (it,sup) =>
      println(it.mkString(",") + "\t" + sup)
    }
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

  object Combinations {

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
  
  class SetAccumulatorParam[T:ClassTag] extends AccumulatorParam[Set[T]] {
    def zero(initialValue: Set[T]): Set[T] = {
      initialValue
    }
    def addInPlace(v1: Set[T], v2: Set[T]): Set[T] = {
      v1.union(v2)
    }
  }

  case class PrefixPartitioner(numPartitions: Int) extends Partitioner {

    //def binCoeff(n: Int, k: Int) = {
    //  var i = n
    //  var coeff = 1
    //  while (i > (n-k)) {
    //    coeff *= i
    //    i -= 1
    //  }
    //  (2 to k).foreach (j => coeff /= j)
    //  coeff
    //}

    def getPartition(key: Any): Int = {
      key.asInstanceOf[Vector[Int]].dropRight(1)
        .mkString("").hashCode.abs % numPartitions
    }

    override def equals(other: Any): Boolean = {
      other.isInstanceOf[PrefixPartitioner]
    }
  }

  def printHelp = {
    println("Usage:\n$ ./bin/spark-submit --class fim.eclat.eclat " +
      " <jar_file> <input_file> <min_support> <item_separator> <num_partitions>\n")
  }

}

