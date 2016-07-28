package br.ufmg.cs.lib.mlearning

import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf

import org.apache.spark.Logging

import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.clustering.custom.{KMeans => MLLibKMeans}

class KMeans (
      inputFile: String,
      k: Int,
      iterations: Int,
      numPartitionsData: Int,
      numPartitionsReduction: Int)
  extends Logging {

  def run {
    val conf = new SparkConf().setAppName("KMeans")
    val sc = new SparkContext(conf)

    val data = sc.textFile(inputFile, numPartitionsData)
    val parsedData = data.
      map(s => Vectors.dense(s.split(' ').drop (1).map(_.toDouble))).
      cache

    val clusters = MLLibKMeans.train(parsedData, numPartitionsReduction, k, iterations, 1, MLLibKMeans.RANDOM)

    val error = clusters.computeCost(parsedData)
    logInfo ("Sum of Squared Errors = " + error)

    sc.stop()
  }
}

object KMeans {
  def main(args: Array[String]) {
    new KMeans(args(0), args(1).toInt, args(2).toInt, args(3).toInt, args(4).toInt).run
  }
}
