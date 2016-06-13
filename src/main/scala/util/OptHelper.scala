package util

import util.PrivateMethodExposer.p

import org.apache.spark._
import org.apache.spark.rdd._
import org.apache.spark.serializer.Serializer

import scala.reflect.ClassTag

trait Action

case class UNPAction(numPartitions: Int) extends Action
case class UPAction(partitionerStr: String) extends Action
case object NOAction extends Action

/**
 */
class OptHelper(apToActions: Map[String,Array[Action]]) extends Logging {
  var currPos = Map.empty[String,Int].withDefaultValue (0)

  def this() = {
    this (Map.empty[String,Array[Action]])
  }

  private def nextPos(point: String) = {
    val pos = currPos (point)
    if (pos < apToActions(point).size) {
      currPos += (point -> (pos+1))
      pos
    } else {
      logWarning (s"Execution plan is too small(${point},${pos},${currPos.size}), reusing last action.")
      apToActions.size - 1
    }
  }

  private def getPartitioner[K: ClassTag, V: ClassTag](rdd: RDD[(K,V)],
      point: String)(implicit orderingOpt: Option[Ordering[K]] = None) = apToActions(point)(nextPos(point)) match {
    case NOAction =>
      rdd.partitioner.get
    case UNPAction(numPartitions) =>
      new HashPartitioner(numPartitions)
    case UPAction("rangePartitioner") if orderingOpt.isDefined =>
      implicit val ordering = orderingOpt.get
      new RangePartitioner(rdd.partitioner.get.numPartitions,
        rdd.dependencies.head.rdd.asInstanceOf[RDD[_ <: Product2[K,V]]])
    case UPAction("hashPartitioner") =>
      new HashPartitioner(rdd.partitioner.get.numPartitions)
    case action =>
      throw new RuntimeException (s"Unrecognized Action: ${action}")
  }

  def adaptRDD[K: ClassTag, V: ClassTag, C: ClassTag](
      point: String,
      rdd: RDD[(K,C)],
      prev: RDD[(K,V)]): RDD[(K,C)] = rdd match {
    case shRdd: ShuffledRDD[K,V,C] =>
      adaptShuffledRDD (point, shRdd, shRdd.prev).
        setName (point)
    case cgRdd: CoGroupedRDD[K] => 
      adaptCoGroupedRDD (point, cgRdd)
    case _ =>
      rdd.setName (point)
  }

  /** adapt CoGroupedRDD */
  def adaptCoGroupedRDD[K: ClassTag](point: String, rdd: CoGroupedRDD[K]) = apToActions.get (point) match {
    case Some(actions) =>
      val cgRdd = new CoGroupedRDD[K](rdd.rdds, getPartitioner(rdd, point)).
        setSerializer (p(rdd)('serializer)().asInstanceOf[Serializer])
      cgRdd
    case None =>
      rdd
  }
  
  /** adapt ShuffledRDD */
  def adaptShuffledRDD[K: ClassTag, V: ClassTag, C: ClassTag](
      point: String, rdd: ShuffledRDD[K,V,C],
      prev: RDD[_ <: Product2[K,V]]) = apToActions.get (point) match {
    case Some(actions) =>
      implicit val keyOrderingOpt = p(rdd)('keyOrdering)().asInstanceOf[Option[Ordering[K]]]
      val shRdd = new ShuffledRDD[K,V,C](rdd.prev, getPartitioner(rdd, point)).
        setSerializer (
          p(rdd)('serializer)().asInstanceOf[Option[Serializer]].getOrElse(null)
        ).
        setKeyOrdering (keyOrderingOpt.getOrElse(null)).
        setAggregator (
          p(rdd)('aggregator)().asInstanceOf[Option[Aggregator[K,V,C]]].getOrElse(null)
        ).
        setMapSideCombine (
          p(rdd)('mapSideCombine)().asInstanceOf[Boolean]
        )
      shRdd
    case None =>
      rdd
  }

  override def toString = s"OptHelper(${apToActions.map(kv => s"${kv._1}:${kv._2.mkString(",")}")})"
}

object OptHelper {

  def apply(actionsStr: String) = {

    var apToActions = Map[String,Array[Action]]().withDefaultValue(Array.empty)

    actionsStr.split(";").map (_.split(",")).foreach {
      case values if values(0) == "act-unp" =>
        apToActions += (values(1) -> (apToActions(values(1)) :+ UNPAction(values(2).toInt)))
      case values if values(0) == "act-up" =>
        apToActions += (values(1) -> (apToActions(values(1)) :+ UPAction(values(2))))
      case values if values(0) == "no-act" =>
        apToActions += (values(1) -> (apToActions(values(1)) :+ NOAction))
      case values =>
        throw new RuntimeException (s"Unrecognized Action: ${values(0)}")
    }

    new OptHelper(apToActions)
  }

}
