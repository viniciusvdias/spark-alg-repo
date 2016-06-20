package dmining.fim.eclat

// command line parsing
import util.ParamsParser

// common
import util.Common

// log
import org.apache.log4j.{Level, Logger}

// spark
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.{Partitioner, SparkConf, SparkContext}
import org.apache.spark.SparkContext._
import org.apache.spark.storage.StorageLevel
import org.apache.spark.rdd.RDD

// serializer
import com.esotericsoftware.kryo.{Serializer, Kryo}
import org.apache.spark.serializer.KryoRegistrator

// scala stuff (collections, recursion, etc.)
import scala.annotation.tailrec
import scala.collection.mutable.{Map, ArrayBuffer}
import scala.reflect.ClassTag

import br.ufmg.cs.systems.sparktuner.OptHelper
import br.ufmg.cs.systems.sparktuner.rdd.AdaptableFunctions._

object Eclat {

  /**
   * @param inputFile path to transactions file
   * @param minSupport minimum percentage to consider an itemset frequent
   * @param numPartitions sequence of numPartitions for reduce steps
   * @param sep item delimiter
   * @param logLevel level of log4j 
   */
  case class Params(inputFile: String = null,
      minSupport: Double = 0.5,
      optHelper: OptHelper = new OptHelper,
      numPartitions: Array[Int] = Array(128),
      sep: String = "\\s+",
      logLevel: Level = Level.OFF) {

    def getValues = (inputFile, minSupport, numPartitions, optHelper,
      sep, logLevel)

    override def toString =
      "&%@ params " + getValues.productIterator.mkString (" ")
  }

  val appName = "FP-Eclat"
  val log = Logger.getLogger(appName)

  // typedefs and strRepr for ease
  type ItemSet = ArrayBuffer[Int]
  type TList = Array[Int]
  def candAsString(candidatesRDD: RDD[(ItemSet,TList)]) = {
    "$$ " +
    candidatesRDD.collect.map {case (it,tids) =>
      it.mkString(",") + "::" + tids.mkString(",")
    }.mkString("\t")
  }
  def writeItemSets[T : ClassTag](itemSetRDD: RDD[(T,Int)], fileName: String) = {
    itemSetRDD.map {
      case (it,sup) if it.isInstanceOf[Int] => it + " " + sup
      case (it,sup) if it.isInstanceOf[ItemSet] =>
        it.asInstanceOf[ItemSet].mkString(",") + " " + sup
    }.saveAsTextFile (fileName)
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
      s"percentage for considering itemsets frequent, default: ${defaultParams.minSupport}",
      (value,tmpParams) => tmpParams.copy (minSupport = value.toDouble)
      )

    parser.opt ("numPartitions",
      s"number of partitions (reduce), default: ${defaultParams.numPartitions.mkString("[",",","]")}",
      (value,tmpParams) => tmpParams.copy (numPartitions = value.split(",").map(_.toInt))
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

  def getTidLists(
      transactionsRDD: RDD[Array[Int]],
      freqsBc: Broadcast[scala.collection.Map[Int,Int]]) = {

    val tidListsRDD = transactionsRDD
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

        tids.toArray.sortBy (_._1).map {case (it,tList) =>
          (ArrayBuffer(it),tList.toArray)
        }.iterator
      }

    tidListsRDD
  }

  /*    
   * intersects two sorted lists (ascending order)
   */
  def ordIntersect(list1: Array[Int], list2: Array[Int]) = {
    val intersect = ArrayBuffer.empty[Int]

    var (i,j) = (0,0)
    while (i < list1.size && j < list2.size) {
      // prefix
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

  def newCandidates(tidListsRDD: RDD[(ItemSet,TList)], level: Int) = {
    
    val candidatesRDD = tidListsRDD.mapPartitions ({tSetIter =>
      val candidates = ArrayBuffer.empty[(ItemSet,TList)]
      val tids = tSetIter.toArray

      def compareItemSet (it1: ItemSet, it2: ItemSet) = {
        var i = 0
        while (it1(i) == it2(i)) i += 1
        if (i < it1.size && i < it2.size)
          it1(i) compare it2(i)
        else
          it1.size compare it2.size
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
          if (prefix == it2.dropRight(1) && compareItemSet (it1, it2) < 0) { // same eqv. class, same prefix
            // candidate
            candidates.append( (it1 :+ it2.last, ordIntersect(tids1,tids2)) )
          } else break = true // here begins a new equivalence class, compare later
          j += 1
        }
        i += 1
      }
      candidates.iterator
    }, true) // preserve original partitioning of partial tLists
    .persist(StorageLevel.MEMORY_AND_DISK_SER)
    .setName (s"candidate_level_${level}")

    candidatesRDD
  }

  def run(params: Params, confOpt: Option[SparkConf] = None) {
    // params as vals (lazy evaluation safety)
    val (inputFile, minSupport,
      reducePlan, optHelper, sep, logLevel) = params.getValues
    val timestamp = System.currentTimeMillis

    // set log levels
    log.setLevel(logLevel)
    Logger.getLogger("org").setLevel(logLevel)
    Logger.getLogger("akka").setLevel(logLevel)
    Logger.getLogger("br").setLevel(logLevel)

    log.info (s"\n\n${params}\n")

    // create config
    val conf = confOpt.getOrElse (new SparkConf().setAppName(appName))
    conf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")

    // make spark context
    val sc = new SparkContext(conf)
    preAdapt (sc)
    log.info (s"applicationId = ${sc.applicationId}")
    
    // start timer
    var t0 = System.nanoTime

    var reduceIdx = 0
    def nextReducePlan = {
      val numPartitions = reducePlan(reduceIdx)
      if (reduceIdx < reducePlan.size - 1)
        reduceIdx += 1
      numPartitions
    }


    // raw file input format and database partitioning
    val linesRDD = sc.textFile(inputFile, nextReducePlan,
      "adaptive-point-input")

    // format lines into transactions
    val transactionsRDD = getTransactions (linesRDD, sep) 
    
    // get transaction's count to estimate minSupport count
    val transCount = transactionsRDD.count
    log.info ("transaction count = " + transCount)
    
    // min count
    val minCount = (minSupport * transCount).toInt
    log.info ("min support count = " + minCount)

    // 1-itemset counting and broadcast to first prunning
    val frequencyRDD = transactionsRDD.
      flatMap {_.iterator zip Iterator.continually(1)}.
      reduceByKey (_ + _, nextReducePlan,
        "adaptive-point-1itemset-counting").
      filter {case (_,sup) => sup > minCount}.
      persist (StorageLevel.MEMORY_ONLY_SER).
      setName ("frequency_rdd")

    var level = 1
    val sufix = "_%s_%s_%s.eclat"
      .format(inputFile.split("/").last,minSupport,reducePlan.mkString(","))

    writeItemSets (frequencyRDD,
      s"${timestamp}_${level}-itemsets${sufix}"
      )
    frequencyRDD.unpersist()

    // broadcast frequent 1-itemsets
    var freqs = frequencyRDD.collectAsMap
    var freqsBc = sc.broadcast (freqs)
   
    // inverted lists
    var tidListsRDD = getTidLists (transactionsRDD, freqsBc)
   
    val itemSets = ArrayBuffer.empty[RDD[(ItemSet,Int)]]
    var nItens = freqsBc.value.size
    var nFreqs = nItens
    var oldCandidatesRDD: RDD[(ItemSet,TList)] = null
    var oldBc: Broadcast[scala.collection.Map[ItemSet,Int]] = null

            
    while (nItens > 1) {
      log.info (nItens + " frequent " + level + "-itemsets ...")

      // gen candidates by intersecting local tidLists
      val candidatesRDD = newCandidates (tidListsRDD, level)

      // aggregate Tlist sizes for each itemset candidate (i.e. counting)
      // preserve only the frequent ones
      val countsRDD = candidatesRDD.
        aggregateByKey (0, nextReducePlan, "adaptive-point-global-counting") (
          (c,tids) => c + tids.size,
          (c1,c2) => c1 + c2
        ).filter {case (_,sup) => sup > minCount}

      itemSets.append (countsRDD.persist (StorageLevel.MEMORY_ONLY_SER))
      writeItemSets (countsRDD,
        s"${timestamp}_${level+1}-itemsets${sufix}"
        )

      // broadcast frequent k-itemsets
      val newFreqsBc = sc.broadcast (countsRDD.collectAsMap)
      nItens = newFreqsBc.value.size

      // broacast and RDD storage cleaning from previous stage
      if (oldCandidatesRDD != null) {
        log.info (level + ". " + Common.storageInfo (oldCandidatesRDD))
        oldCandidatesRDD.unpersist()
        oldCandidatesRDD = candidatesRDD
      } else oldCandidatesRDD = candidatesRDD

      if (level == 1) {
        freqsBc.unpersist()
        log.info (Common.storageInfo (transactionsRDD))
        transactionsRDD.unpersist()
      }

      if (oldBc != null) {
        oldBc.unpersist()
        oldBc = newFreqsBc
      } else oldBc = newFreqsBc

      // frequent candidates become new itemsets
      tidListsRDD = candidatesRDD.mapPartitions (
        _.filter {case (it,_) => newFreqsBc.value.contains(it)}, true
      )
     
      // new stage
      nFreqs += nItens
      level += 1
    }

    logLevel match {
      case Level.OFF =>
        // print found frequent itemsets
        frequencyRDD.foreach {case (it,sup) => println(it + "\t" + sup)}
        itemSets.foreach (_.foreach {
          case (it,sup) => println(it.mkString(",") + "\t" + sup)}
        )

      case _ =>
        val currTime = timestamp
        val sufix = "_%s_%s_%s_%s.eclat"
          .format(inputFile.split("/").last,minSupport,
            reducePlan.mkString(","))

        val levels = Iterator.from(1)
        frequencyRDD.map {case (it,sup) => it + " " + sup}
          .saveAsTextFile (currTime + "_" + levels.next + "-itemsets" + sufix)


        itemSets.foreach (_.map {case (it,sup) => it.mkString(",") + " " + sup}
          .saveAsTextFile (currTime + "_" + levels.next + "-itemsets" + sufix)
        )
    }
            
    log.info ("All execution took " + (System.nanoTime - t0) / (1000000000.0) + " seconds")
    sc.stop()
  }
}

