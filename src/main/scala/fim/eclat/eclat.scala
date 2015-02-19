package fim.eclat

// log
import org.apache.log4j.Level
import org.apache.log4j.Logger

// spark
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.Partitioner
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext._
import org.apache.spark.storage.StorageLevel
import org.apache.spark.{Logging, SparkContext}
import org.apache.spark.rdd.RDD

// serializer
import com.esotericsoftware.kryo.Serializer
import org.apache.spark.serializer.KryoRegistrator
import com.esotericsoftware.kryo.Kryo

// scala stuff
import scala.annotation.tailrec
import scala.collection.mutable.Map
import scala.collection.mutable.ArrayBuffer

import scala.reflect.ClassTag

object eclat {

  type ItemSet = ArrayBuffer[Int]
  type TList = Array[Int]

  val log = Logger.getLogger("eclat")
  log.setLevel(Level.DEBUG)

  // default input parameters
  var inputFile = ""
  var sep = " "
  var minSup = 1
  var numberOfPartitions = 128

  val conf = new SparkConf().setAppName("FP-Eclat")
  conf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
  
  def main(args: Array[String]) {
    
    // command line arguments
    var minPctSup = minSup.toDouble
    try {
      inputFile = args(0)
      minPctSup = args(1).toDouble
      sep = args(2)
      numberOfPartitions = args(3).toInt

      args(4) match {
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
    
    // start time
    var t0 = System.nanoTime
   
    // raw file input format and database partitioning
    val linesRDD = sc.textFile(inputFile, numberOfPartitions)
    val sepBc = sc.broadcast(sep)

    // format transactions
    var transactionsRDD = linesRDD
      .map(l => (l split sepBc.value).map(it => it.toInt))
      .persist (StorageLevel.MEMORY_ONLY)
    
    // get transaction's count to estimate minSupport count
    val transCount = transactionsRDD.count.toInt
    val tCountBc = sc.broadcast(transCount)
    log.debug("transaction count = " + transCount)

    // minimum support percentage to support count
    minSup = (minPctSup * transCount).toInt
    val minSupBc = sc.broadcast(minSup)
    log.debug("support count = " + minSup)
    
    // 1-itemset counting and broadcast to first prunning
    val frequencyRDD = transactionsRDD
      .flatMap (t => t.iterator zip Iterator.continually(1))
      .reduceByKey (_ + _)
      .filter {case (_,sup) => sup > minSupBc.value}
      .persist (StorageLevel.MEMORY_ONLY_SER)
    var freqsBc = sc.broadcast (frequencyRDD.collectAsMap)
    
    val itemSets = ArrayBuffer.empty[RDD[(ItemSet,Int)]]

    var nItens = freqsBc.value.size
    var nFreqs = nItens
    var level = 1

    var tidsRDD = transactionsRDD
      .map (_.filter (it => freqsBc.value.contains(it)))
      .zipWithUniqueId()
      .mapPartitions {transIter =>
        val tids = Map.empty[Int,ItemSet]

        while (transIter.hasNext) {
          val (t,tid) = transIter.next
          
          val itIter = t.iterator
          while (itIter.hasNext) {
            val list = tids.getOrElseUpdate(itIter.next, ArrayBuffer.empty)
            list += tid.toInt
          }
        }

        tids.toArray.sortBy (_._1).map {case (it,tList) => (ArrayBuffer(it),tList.toArray)}.iterator
      }

    var oldCandidatesRDD: RDD[(ItemSet,TList)] = null
    var oldBc: Broadcast[scala.collection.Map[ItemSet,Int]] = null

    while (nItens > 1) {
      log.debug (nItens + " frequent " + level + "-itemsets ...")

      val candidatesRDD = tidsRDD.mapPartitions ({tSetIter =>
        val candidates = ArrayBuffer.empty[(ItemSet,TList)]
        val tids = tSetIter.toArray
        
        // intersects two sorted lists (ascending order)
        def ordIntersect(list1: Array[Int], list2: Array[Int]) = {
          val intersect = ArrayBuffer.empty[Int]

          var (i,j) = (0,0)
          while (i < list1.size && j < list2.size) {
            // elements co-occur
            if ( list1(i) == list2(j) ) {
              intersect += list1(i)
              i += 1
              j += 1
            }
            // current element from *list1* could not appear in the future
            else if ( list1(i) < list2(j) ) i += 1
            else j += 1
          }
          intersect.toArray // implicit convertion
        }

        // 2-combinations of (k-1)-itemsets with the same prefix (eqv. class)
        var i = 0
        while (i < tids.size) {
          val (it1,tids1) = tids(i)
          val prefix = it1.dropRight(1)

          var j = i+1
          var break = false
          while (j < tids.size && !break) {
            val (it2,tids2) = tids(j)
            if (prefix == it2.dropRight(1)) { // same eqv. class, same prefix
              // candidate
              candidates.append( (it1 :+ it2.last, ordIntersect(tids1,tids2)) )
            } else break = true // here begins a new equivalence class, compare later
            j += 1
          }
          i += 1
        }
        candidates.iterator
      }, true // preserve original partitioning of partial tLists
      ).persist(StorageLevel.MEMORY_ONLY)

      // aggregate Tlist sizes for each itemset candidate (i.e. counting)
      // preserve only the frequent ones
      val countsRDD = candidatesRDD.mapValues {tids => tids.size}
        .reduceByKey (_ + _)
        .filter {case (_,sup) => sup > minSupBc.value}
      itemSets.append (countsRDD.persist (StorageLevel.MEMORY_ONLY_SER))

      // broadcast frequent k-itemsets
      val newFreqsBc = sc.broadcast (countsRDD.collectAsMap)
      nItens = newFreqsBc.value.size

      // broacast and RDD storage cleaning from previous stage
      if (oldCandidatesRDD != null) {
        oldCandidatesRDD.unpersist()
        oldCandidatesRDD = candidatesRDD
      } else oldCandidatesRDD = candidatesRDD

      if (level == 1) {
        freqsBc.unpersist()
        transactionsRDD.unpersist()
      }

      if (oldBc != null) {
        oldBc.unpersist()
        oldBc = newFreqsBc
      } else oldBc = newFreqsBc

      // frequent candidates become new itemsets
      tidsRDD = candidatesRDD.mapPartitions (_.filter {case (it,_) => newFreqsBc.value.contains(it)}, true)
     
      // new stage
      nFreqs += nItens
      level += 1
    }

    args(4) match {
      case "log" =>
        val currTime = System.currentTimeMillis
        val sufix = "_%s_%s_%s.eclat"
          .format(inputFile.split("/").last,minPctSup,numberOfPartitions)

        val levels = Iterator.from(1)
        frequencyRDD.map {case (it,sup) => it + " " + sup}
          .saveAsTextFile (currTime + "_" + levels.next + "-itemsets" + sufix)

        itemSets.foreach (_.map {case (it,sup) => it.mkString(",") + " " + sup}
          .saveAsTextFile (currTime + "_" + levels.next + "-itemsets" + sufix)
        )

      case "nolog" =>
        // print found frequent itemsets
        println(":: Itemsets " + nFreqs)
        frequencyRDD.foreach {case (it,sup) => println(it + "\t" + sup)}
        itemSets.foreach (_.foreach {
          case (it,sup) => println(it.mkString(",") + "\t" + sup)}
        )
    }
            
    log.debug ("All execution took " + (System.nanoTime - t0) / (1000000000.0) + " seconds")

  }

  def candAsString(candidatesRDD: RDD[(ItemSet,TList)]) = {
    "$$ " +
    candidatesRDD.collect.map {case (it,tids) =>
      it.mkString(",") + "::" + tids.mkString(",")
    }.mkString("\t")
  }

  def printHelp = {
    println("Usage:\n$ ./bin/spark-submit --class fim.eclat.eclat " +
      " <jar_file> <input_file> <min_support> <item_separator> <num_partitions>\n")
  }

}

