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

object ByteCountPerKeyAgg {
  val appName = "ByteCountPerKeyAgg"
  val log = Logger.getLogger(appName)

  case class Params(inputFiles: Array[String] = Array.empty,
      logLevel: Level = Level.OFF) {

    def getValues = (inputFiles, logLevel)

    override def toString =
      "&%@ params " + getValues.productIterator.mkString (" ")
  }

  def main(args: Array[String]) {
  
    val defaultParams = Params()

    val parser = new ParamsParser[Params](appName)
    parser.opt ("inputFiles",
      s"path to input files, default: ${defaultParams.inputFiles}",
      (value,tmpParams) => tmpParams.copy (inputFiles = value.split(";")),
      true
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

    val (inputFiles, logLevel) = params.getValues

    // set log levels
    log.setLevel(logLevel)

    val conf = new SparkConf().setAppName(appName).
      set ("spark.instrumentation.writer.bykey", "false")

    val sc = new SparkContext(conf)
    log.debug (s"applicationId = ${sc.applicationId}")

    val futures = inputFiles.map { inputFile => Future {

      val kvv = sc.textFile (inputFile).
        map (_ split "\\s+").
        map (arr => (arr.dropRight(1).mkString(""), (1L, arr.last.toLong))).
        reduceByKey { case ((n1,b1), (n2,b2)) => (n1+n2, b1+b2) }.
        map { case (k, (n,b)) => s"${k}\t${n}\t${b}" }
      
      val aggFile = s"${inputFile.replaceAll("\\*","")}.agg"
      log.info (s"${inputFile}: saving aggregated results to ${aggFile}")
      kvv.saveAsTextFile (aggFile)
      aggFile
    }}.toSeq

    val completionFuture = Future.sequence (futures)
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
