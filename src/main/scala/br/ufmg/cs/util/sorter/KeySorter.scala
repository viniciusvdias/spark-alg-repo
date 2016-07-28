package br.ufmg.cs.util.sorter

// command line parsing
import br.ufmg.cs.util.ParamsParser

// utils
import br.ufmg.cs.util.Common

import org.apache.log4j.{Logger, Level}

// spark
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.SparkContext._
import org.apache.spark.storage.StorageLevel
import org.apache.spark.rdd.RDD

import org.apache.hadoop.io._
import org.apache.hadoop.fs._

import scala.concurrent._
import scala.concurrent.duration._
import scala.concurrent.ExecutionContext.Implicits.global
import scala.util.{Try, Success, Failure}

object KeySorter {
  val appName = "KeySorter"
  val log = Logger.getLogger(appName)

  case class Params(inputFile: String = null,
      logLevel: Level = Level.OFF) {

    def getValues = (inputFile, logLevel)

    override def toString =
      "&%@ params " + getValues.productIterator.mkString (" ")
  }

  def main(args: Array[String]) {
  
    val defaultParams = Params()

    val parser = new ParamsParser[Params](appName)
    parser.opt ("inputFile",
      s"path to transactions input file, default: ${defaultParams.inputFile}",
      (value,tmpParams) => tmpParams.copy (inputFile = value)
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

  def run(params: Params) {

    val (inputFile, logLevel) = params.getValues

    // set log levels
    log.setLevel(logLevel)

    val conf = new SparkConf().setAppName(appName).
      set ("spark.instrumentation.keydist", "false")

    val sc = new SparkContext(conf)
    sc.hadoopConfiguration.set ("mapreduce.input.fileinputformat.input.dir.recursive", "true")

    val fs = FileSystem.get (sc.hadoopConfiguration)
    log.debug (s"applicationId = ${sc.applicationId}")

    var jobFutures = List.empty[Future[Unit]]

    val appsPath = new Path ("keydist/*")
    val appStatuses = fs.globStatus (appsPath)
    for (appStatus <- appStatuses) {
      var lens = Map.empty[Long,String]
      val stagesPath = new Path (appStatus.getPath, "*")
      val stageStatuses = fs.globStatus (stagesPath)
      for (stageStatus <- stageStatuses) {
        val depsPath = new Path (stageStatus.getPath, "*")
        val depStatuses = fs.globStatus (depsPath)
        for (depStatus <- depStatuses) {
          val parts = fs.globStatus (new Path(depStatus.getPath, "*"))
          val newLen = parts.map (_.getLen).sum
          if (!lens.contains(newLen)) {
            // sort for all partitions
            jobFutures = Future {
              log.info (s"${newLen} reading for dependency ${depStatus.getPath.toString}")
              val keys = sc.textFile (depStatus.getPath.toString)
              val freqs = keys.map (k => (k,1L)).reduceByKey (_ + _)
              freqs.sortByKey().map (tup => s"${tup._1} ${tup._2}").saveAsTextFile (
                depStatus.getPath.toString.replaceAll ("keydist", "keydist-aggregated")
              )
              log.info (s"${newLen} finished for dependency ${depStatus.getPath.toString}")
            } :: jobFutures
            lens += (newLen -> depsPath.toString)
          } else {
            log.info (s"${newLen} skipping dependency ${depStatus.getPath.toString}")
          }
        }
      }
    }

    val completionFuture = Future.sequence (jobFutures)
    Await.ready (completionFuture, Duration.Inf)
    completionFuture.value.get match {
      case Success(_) =>
        println (s"Success !!")
      case Failure(e) =>
        println (s"Failure !!")
        throw e
    }

    sc.stop

  }
}
