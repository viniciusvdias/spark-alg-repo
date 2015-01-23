package fim.fptree

import fim.fptree._

import scala.annotation.tailrec

import scala.collection.mutable.Map
import scala.collection.mutable.ListBuffer
import scala.collection.mutable.HashMap
import scala.collection.mutable.Map

object Node {
  def emptyNode = Node(0, 0, null, 0)
  def labeledEmptyNode(rootId: Int) = Node(rootId, 0, null, 0)
  private var currentId = 0
  private def nextId = {currentId += 1; currentId}
}

/** Frequent pattern tree node.
 * 
 * @constructor create a new node with an item, its frequency on the DB
 * and a parent.
 * @param _itemId unique item ID
 * @param _count how frequent is this item
 * @param _parent another [[fptree.Node]] or null whether it is root
 */
case class Node(var itemId: Int,
    var count: Int,
    var parent: Node,
    var level: Int,
    var uniqId: Int = Node.nextId,
    var link: Node = null,
    var children: ListBuffer[Node] = ListBuffer[Node](),
    var tids: Int = 0) extends Serializable {

  def isEmpty: Boolean = (itemId == 0)
  
  def isRoot: Boolean = (parent == null)

  def addChild(cs: Node*) = {
    // children ++= cs.map(c => (c.itemId, c)) // if children is a map
    children.appendAll(cs)
    cs.foreach(c => c.parent = this)
  }

  def findChild(childId: Int) = children.filter(_.itemId == childId) match {
    case ListBuffer() => null
    case cs => cs.iterator.next
  }
    //if (children.contains(childId)) children(childId)
    //else null

  def insertTrans(
      trans: Array[Int],
      table: Map[Int,Node],
      count: Int): Node = {
    
    @tailrec
    def insertTransRec(
        trans: Array[Int],
        tree: Node,
        table: Map[Int,Node],
        count: Int): Node = trans match {

      case Array() =>
        tree.tids += 1
        tree

      case htail => 

        val (t, ts) = (htail.head, htail.tail)
        val child = tree.findChild(t)

        child match {
          case null =>
            if (!table.contains(t))
              table(t) = null

            val firstNode = table(t)
            val newNode = Node(t, count, tree, tree.level + 1)
            newNode.link = firstNode
            table(t) = newNode
            tree.addChild(newNode)
            insertTransRec(ts, newNode, table, count)

          case c =>
            c.count += count
            insertTransRec(ts, c, table, count)
      }
    }
    insertTransRec(trans, this, table, count)
  }

  override def toString = {
    def toStringRec(tree: Node, level: Int): String = {
      var str = "(tree=" + System.identityHashCode(tree) + ", uniqId=" + tree.uniqId + ", itemId=" + tree.itemId + ", count=" + tree.count

      if (tree.parent != null) str += ", parent=" + tree.parent.itemId
      else str += ", parent=-1"
      str += ", level=" + tree.level
      if (tree.link != null) str += ", link=(" + tree.link.uniqId + "," +
      tree.link.itemId + ", hash=" + System.identityHashCode(tree.link) + ")"
      else str += ", link=-1"

      str += ", #children=" + tree.children.size
      str += ", tids=" + tree.tids + ")\n"

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
