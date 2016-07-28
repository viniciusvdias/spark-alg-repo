package br.ufmg.cs.util

// log
import org.apache.log4j.{Level, Logger}

// spark
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.{Partitioner, SparkConf, SparkContext}
import org.apache.spark.SparkContext._
import org.apache.spark.storage.StorageLevel
import org.apache.spark.rdd.RDD

import scala.collection.Traversable

import scala.reflect.ClassTag

object Common {

  /*
   * get storage info (e.g. space in memory) of a given RDD
   */
  def storageInfo[T : ClassTag](rdd: RDD[T]) = {
    val debugString = rdd.toDebugString 
    s"storage info = ${debugString.substring (debugString.indexOf("CachedPartitions:"))}"
  }

  /*
   * get a database sample
   */
  def genDatabaseFraction[T : ClassTag](dbRDD: RDD[T], fraction: Double, seed: Long) = {
    val dbSampleRDD = dbRDD.sample (false, fraction, seed)
    dbSampleRDD
  }
}
