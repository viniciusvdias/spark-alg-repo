/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.spark.rdd

import java.io.{IOException, ObjectOutputStream}

import scala.reflect.ClassTag

import org.apache.spark._
import org.apache.spark.util.Utils

private[spark]
class CombinationPartition(
    idx: Int,
    @transient rdd1: RDD[_],
    @transient rdd2: RDD[_],
    s1Index: Int,
    s2Index: Int
  ) extends Partition {
  var s1 = rdd1.partitions(s1Index)
  var s2 = rdd2.partitions(s2Index)
  override val index: Int = idx

  @throws(classOf[IOException])
  private def writeObject(oos: ObjectOutputStream): Unit = Utils.tryOrIOException {
    // Update the reference to parent split at the time of task serialization
    s1 = rdd1.partitions(s1Index)
    s2 = rdd2.partitions(s2Index)
    oos.defaultWriteObject()
  }
}

class CombinationRDD[U <% Ordered[U] : ClassTag, W : ClassTag, T : ClassTag](
    sc: SparkContext,
    var rdd1 : RDD[T],
    var keyFunc: T => U = (it: T) => it.asInstanceOf[U],
    var prefixFunc: T => W = (it: T) => it.asInstanceOf[W],
    var perPartition: Boolean = false
  )
  extends RDD[Pair[T, T]](sc, Nil)
  with Serializable {
  
  val nParts = rdd1.partitions.size

  override def getPartitions: Array[Partition] = perPartition match {
    case true =>
      val array = new Array[Partition](rdd1.partitions.size)
      for (s1 <- rdd1.partitions) {
        val idx = s1.index
        array(idx) = new CombinationPartition(idx, rdd1, rdd1, idx, idx)
      }
      array

    case false =>   
      // create the cross product split
      //val array = new Array[Partition](nParts*(nParts-1)/2 + nParts)
      val array = new Array[Partition](nParts * nParts)
      //def pIdx(i: Int, j: Int, n: Int) = (i*(n-(i+1)/2.0) + j).toInt
      for (s1 <- rdd1.partitions; s2 <- rdd1.partitions
           //; if s1.index <= s2.index
           ) {
        val idx = s1.index * nParts + s2.index
        //val idx = pIdx(s1.index, s2.index, nParts) 
        array(idx) = new CombinationPartition(idx, rdd1, rdd1, s1.index, s2.index)
      }
      array
  }

  override def getPreferredLocations(split: Partition): Seq[String] = {
    val currSplit = split.asInstanceOf[CombinationPartition]
    (rdd1.preferredLocations(currSplit.s1) ++ rdd1.preferredLocations(currSplit.s2)).distinct
  }

  override def compute(split: Partition, context: TaskContext) = {
    val currSplit = split.asInstanceOf[CombinationPartition]
    for {x <- rdd1.iterator(currSplit.s1, context);
         y <- rdd1.iterator(currSplit.s2, context);
         if prefixFunc(x) == prefixFunc(y) &&
          keyFunc(x) < keyFunc(y)
         } yield (x, y)
  }

  override def getDependencies: Seq[Dependency[_]] = List(
    new NarrowDependency(rdd1) {
      def getParents(id: Int): Seq[Int] = List(id / nParts)
    },
    new NarrowDependency(rdd1) {
      def getParents(id: Int): Seq[Int] = List(id % nParts)
    }
  )

  override def clearDependencies() {
    super.clearDependencies()
    rdd1 = null
    //rdd2 = null
  }
}
