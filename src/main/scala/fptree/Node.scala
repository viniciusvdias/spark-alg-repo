package fptree

import scala.collection.mutable.Map
import scala.collection.mutable.ListBuffer
import scala.collection.mutable.HashMap
import scala.collection.immutable.Stack

object Node {
  def emptyNode = Node("#", 0, null, 0)
  def labeledEmptyNode(rootId: String) = Node(rootId, 0, null, 0)
}

/** Frequent pattern tree node.
 * 
 * @constructor create a new node with an item, its frequency on the DB
 * and a parent.
 * @param _itemId unique item ID
 * @param _count how frequent is this item
 * @param _parent another [[fptree.Node]] or null whether it is root
 */
case class Node(var itemId: String,
    var count: Int,
    var parent: Node,
    var level: Int,
    var link: Node = null,
    var children: HashMap[String, Node] = HashMap[String, Node](),
    var tids: Int = 0) extends Serializable {

  def isEmpty: Boolean = (itemId == "#")
  
  def isRoot: Boolean = (parent == null)

  def addChild(cs: Node*) = {
    children
    children ++= cs.map(c => (c.itemId, c))
    cs.foreach(c => c.parent = this)
  }

  def findChild(childId: String) =
    if (children.contains(childId)) children(childId)
    else null

  def singlePath = {
    def singlePathRec(node: Node, path: List[Node]): 
        List[Node] = node.children.size match {
      case 0 => path ::: List(node)
      case 1 => singlePathRec(node.children.values.iterator.next, path ::: List(node))
      case _ => Nil
    }
    singlePathRec(this, Nil)
  }

  def insertTrans(
      trans: Array[String],
      table: Map[String,Node],
      count: Int): Node = {

    def insertTransRec(
        trans: Array[String],
        tree: Node,
        table: Map[String,Node],
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
      var str = "(id=" + tree.itemId + ", count=" + tree.count 

      if (tree.tids > 0)
        str += ", tids=" + tree.tids
      str += ", level=" + tree.level + ")\n"

      if (!tree.children.isEmpty) {
        str += tree.children.
        map {case (_,c) => "  " * level + toStringRec(c, level + 1)}.
          reduce(_ + _)
      }
      str
    }
    toStringRec(this, 1)
  }
}
