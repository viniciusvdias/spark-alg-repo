package br.ufmg.cs.lib.ganalytics

import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf

import org.apache.spark.Logging

import org.apache.spark.graphx.GraphLoader
import org.apache.spark.graphx.Edge
import org.apache.spark.graphx.Graph

class ConnectedComponents (inputFile: String, numPartitions: Int, inputType: String) extends Logging {
  def run {
    val conf = new SparkConf().setAppName("ConnectedComponents")
    val sc = new SparkContext(conf)
    sc.setLogLevel ("INFO")

    val graph = ConnectedComponents.loadGraph (sc, inputFile, inputType, numPartitions)
    val cc = graph.connectedComponents()
    logInfo ("There are " + cc.vertices.count() + " components in the graph.")

    sc.stop()
  }
}

object ConnectedComponents {

  val INPUT_EDGES = "edges"
  val INPUT_ADJLISTS = "adjLists"
  
  def loadGraph (sc: SparkContext, inputFile: String, inputType: String, numPartitions: Int) = inputType match {

    case INPUT_EDGES =>
      GraphLoader.edgeListFile(sc, inputFile, false, numPartitions)

    case INPUT_ADJLISTS =>
      val edges = sc.textFile (inputFile, numPartitions).flatMap { line =>
        val vertices = line split "\\s+"
        val from = vertices.head.toLong
        for (i <- 1 until vertices.size)
          yield Edge (from, vertices(i).toLong, 1) 
      }
      Graph.fromEdges (edges, 1)
  }
  def main(args: Array[String]) {
    new ConnectedComponents (args(0), args(1).toInt, args(2)).run
  }
}
