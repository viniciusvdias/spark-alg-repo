package fim.fptree

import fim.fptree._

import scala.annotation.tailrec

import scala.collection.mutable.ListBuffer
import scala.collection.mutable.ArrayBuffer
import scala.collection.mutable.HashSet
import scala.collection.mutable.Map

object Node {
  val Null = null
  val emptyItemId = Int.MinValue
  val emptyChildren = Array.empty[Node]
  def emptyRNode = RNode(emptyItemId, 0)
  def emptyTNode = TNode(emptyItemId, 0)
}

case class TNode(var itemId: Int,
    var count: Int,
    var parent: Node = Node.Null,
    var link: Node = Node.Null,
    var children: Array[Node] = Node.emptyChildren,
    var tids: Int = 0) extends Node

case class RNode(var itemId: Int,
    var count: Int,
    var parent: Node = Node.Null,
    var link: Node = Node.Null,
    var children: Array[Node] = Node.emptyChildren) extends Node

trait Node {
  var itemId: Int
  var count: Int
  var parent: Node
  var link: Node
  var children: Array[Node]

  def isRoot: Boolean = (parent == Node.Null)

  def addChild(c: Node) = {
    children = children :+ c
    c.parent = this
  }

  def findNode(nodeId: Int) = {
    //if (nodeId == this.itemId) this
    //else
    children.find (_.itemId == nodeId) match {
      case Some(node) => node
      case None => null
    }
  }

  //def insertTransaction(transaction: List[Int],
  //    linksTable: Map[Int,Node],
  //    count: Int,
  //    updateLinksTable: Boolean = true) = {

  //  @tailrec
  //  def insertTransactionRec(currNode: Node,
  //      transaction: List[Int],
  //      linksTable: Map[Int,Node],
  //      count: Int,
  //      updateLinksTable: Boolean): Node = transaction match {

  //    case Nil =>
  //      currNode.tids += 1
  //      currNode

  //    case it :: ts => currNode.findNode(it) match {
  //      case null => 
  //        val newNode = Node(it, count, currNode)

  //        if (updateLinksTable) {
  //          val firstNode = linksTable.getOrElse(it, null)
  //          newNode.link = firstNode
  //          linksTable(it) = newNode
  //        }

  //        currNode.addChild(newNode)
  //        insertTransactionRec(newNode, ts, linksTable, count, updateLinksTable)

  //      case c =>
  //        if (c.itemId != this.itemId) c.count += count
  //        insertTransactionRec(c, ts, linksTable, count, updateLinksTable)
  //    }
  //  }

  //  insertTransactionRec(this, transaction, linksTable, count, updateLinksTable)
  //}

  def insertItems(items: Array[Int],
      linksTable: Map[Int,Node],
      count: Int,
      updateLinksTable: Boolean = true,
      isTransaction: Boolean = false) = {

    val transIter = items.iterator
    
    var currNode = this
    while (transIter.hasNext) {
      val it = transIter.next

      currNode.children.find(_.itemId == it) match {
        case None => 
          val newNode =
            if (isTransaction) TNode(it, count)
            else RNode(it, count)

          if (updateLinksTable) {
            val firstNode = linksTable.getOrElse(it, Node.Null)
            newNode.link = firstNode
            linksTable(it) = newNode
          }

          currNode.addChild(newNode)

          currNode = newNode

        case Some(c) =>
          c.count += count
          currNode = c
      }
    }

    currNode match {
      case tNode: TNode => tNode.tids += 1; tNode
      case rNode: RNode => rNode
    }
  }

  override def toString = {
    def toStringRec(tree: Node, level: Int): String = {
      var str = "(tree=" + System.identityHashCode(tree) + ", itemId=" + tree.itemId + ", count=" + tree.count

      if (tree.parent != null) str += ", parent=" + tree.parent.itemId
      else str += ", parent=-1"
      if (tree.link != null) str += ", link=(" +
      tree.link.itemId + ", hash=" + System.identityHashCode(tree.link) + ")"
      else str += ", link=-1"

      str += ", #children=" + tree.children.size
      tree match {
        case tNode: TNode => str += ", tids=" + tNode.tids + ")\n"
        case rNode: RNode => str += ")\n"
      }

      if (!tree.children.isEmpty) {
        str += tree.children.
        map {c => "  " * level + toStringRec(c, level + 1)}.
          reduce(_ + _)
      }
      str
    }
    toStringRec(this, 1)
  }
}
