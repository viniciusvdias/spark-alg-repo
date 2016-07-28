package br.ufmg.cs.lib.ganalytics

import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import org.apache.spark.HashPartitioner

import org.apache.spark.Logging

import org.apache.spark.graphx.GraphLoader
import org.apache.spark.graphx.Edge
import org.apache.spark.graphx.Graph

import br.ufmg.cs.systems.sparktuner.OptHelper

import br.ufmg.cs.systems.sparktuner.rdd.AdaptableFunctions._

class PageRank (
      inputFile: String,
      numPartitions: Int,
      numIterations: Int,
      inputType: String = PageRank.SPARK_PAGERANK,
      _sc: SparkContext = null)
  extends Logging {

  def run {
    val conf = new SparkConf().setAppName("PageRank")
    val sc = Option(_sc).getOrElse (new SparkContext(conf))
    sc.setLogLevel ("INFO")

    val graph = PageRank.loadGraph (sc, inputFile, inputType, numPartitions)
    val ranks = graph.staticPageRank (numIterations, 0.0001).vertices
    logInfo ("" + ranks.count() + " ranks generated.")

    if (_sc == null)
      sc.stop()
  }
}

class SparkPageRank (inputFile: String,
    numPartitions: Array[Int],
    numIterations: Int,
    _sc: SparkContext = null) extends Logging {

  def run {

    val conf = new SparkConf().setAppName("SparkPageRank")
    val sc = Option(_sc).getOrElse (new SparkContext(conf))
    preAdapt (sc)

    val lines = sc.textFile(inputFile, numPartitions(0),
      "adaptive-point-input")
    val links = lines.map { line =>
      val adjList = line.split ("\\s+").map (_.toInt)
      (adjList(0), adjList.drop(1))
    }.cache()

    var ranks = links.mapValues (v => 1.0).cache()
    ranks.count

    for (i <- 1 to numIterations) {
      val start = System.currentTimeMillis
      val _ranks = ranks
      val contribs = links.join(_ranks, numPartitions(1),
          "adaptive-point-contributions").values.
        flatMap { case (urls, rank) =>
          val size = urls.size
          urls.map(url => (url, rank / size))
        }

      ranks = contribs.reduceByKey ((x:Double,y:Double)=>x+y, numPartitions(2),
          "adaptive-point-newrank").
        mapValues(0.15 + 0.85 * _).cache()

      ranks.foreachPartition (_ => {})
      _ranks.unpersist()
      logInfo (s"iteration $i took ${System.currentTimeMillis - start} ms")
    }
    
    logInfo (s"${ranks.count} updated")

    if (_sc == null)
      sc.stop()
  }
}

object PageRank {
  val INPUT_EDGES = "edges"
  val INPUT_ADJLISTS = "adjLists"
  val SPARK_PAGERANK = "sparkPageRank"
  
  def loadGraph (sc: SparkContext, inputFile: String, inputType: String, numPartitions: Int) = inputType match {

    case PageRank.INPUT_EDGES =>
      GraphLoader.edgeListFile(sc, inputFile, false, numPartitions)

    case PageRank.INPUT_ADJLISTS =>
      val edges = sc.textFile (inputFile, numPartitions).flatMap { line =>
        val vertices = line split "\\s+"
        val from = vertices.head.toLong
        for (i <- 1 until vertices.size)
          yield Edge (from, vertices(i).toLong, 1) 
      }
      Graph.fromEdges (edges, 1)
  }

  def main(args: Array[String]) {
    args(3) match {
      case PageRank.SPARK_PAGERANK =>
        // args(3): input type
        // args(2): num iterations
        // args(1): list of partitions (separated by comma) (3 values)
        // args(0): input file
        val numPartitions = (args(1) split ",").map (_.toInt)
        new SparkPageRank (args(0), numPartitions, args(2).toInt).run
      case _ =>
        new PageRank (args(0), args(1).toInt, args(2).toInt, args(3)).run
    }
  }
}
