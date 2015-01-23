package fim.serialization

import fim.fptree._

import com.esotericsoftware.kryo.io.Input
import com.esotericsoftware.kryo.io.Output
import com.esotericsoftware.kryo.Serializer
import org.apache.spark.serializer.KryoRegistrator
import com.esotericsoftware.kryo.Kryo

import scala.collection.concurrent.TrieMap

// registration which avoids default recursive serialization
class SerialRegistrator extends KryoRegistrator {
  override def registerClasses(kryo: Kryo) {
    kryo.register(classOf[FPTree])
    kryo.register(classOf[Node], new NodeSerializer())
  }
}

// default recursive serialization
class DefaultRegistrator extends KryoRegistrator {
  override def registerClasses(kryo: Kryo) {
    kryo.register(classOf[FPTree])
    kryo.register(classOf[Node])
  }
}

object NodeSerializer {
  val intNull = -1
  val endTreeNull = -2
}

class NodeSerializer extends Serializer[Node] {
  val nodes = TrieMap[Int, Node]()
  
  // each node is written as a tuple:
  // (uniqueId, itemId, count, parentId, linkId, tids)
  override def write (k: Kryo, output: Output, obj: Node) = {
    //println("writing begins ..")
    val queue = scala.collection.mutable.Queue[Node](obj)
    while (!queue.isEmpty) {
      val node = queue.dequeue
      node.children.foreach (c => queue += c)
      writeNode(output, node)
    }
    output.writeInt(NodeSerializer.endTreeNull, true)
    //println("writing has been done .. :) " + nNodes)
  }

  def writeNode(output: Output, obj: Node) = {
    output.writeInt(obj.uniqId, true)
    output.writeInt(obj.itemId, true)
    output.writeInt(obj.count, true)
    if (obj.parent != null)
      output.writeInt(obj.parent.uniqId, true)
    else
      output.writeInt(NodeSerializer.intNull, true)
    //if (obj.link != null)
    //  output.writeInt(obj.link.uniqId, true)
    //else
    //  output.writeInt(NodeSerializer.intNull, true)
    output.writeInt(obj.tids, true)
  }

  override def read (kryo: Kryo, input: Input, t: Class[Node]): Node = {
    //println("reads begins .. ")
    var root = readNode(input)
    //println("read root \n" + root)

    var break = false
    var nNodes = 1
    while (!break) {
      val node = readNode(input)
      if (node == null) break = true
      else nNodes += 1
    }
    
    //println("reading has been done .. :) " + nNodes + "\n" + root)
    //println("number of nodes = " + nNodes)
    root
  }

  def readNode(input: Input): Node = {
    val uniqId = input.readInt(true)
    if (uniqId == NodeSerializer.endTreeNull) return null
    val itemId = input.readInt(true)
    val node = {
      try nodes(uniqId)
      catch {
        case e: java.util.NoSuchElementException => Node.emptyNode
      }
    }
    node.itemId = itemId
    node.uniqId = uniqId
    nodes(uniqId) = node

    node.count = input.readInt(true)
    input.readInt(true) match {
      case NodeSerializer.intNull => node.parent = null
      case parentId =>
        val parent = {
          try nodes(parentId)
          catch {
            case e: java.util.NoSuchElementException => Node.emptyNode
          }
        }
        parent.uniqId = parentId
        nodes(parentId) = parent

        node.level = parent.level + 1
        parent.addChild(node)
    }

    //input.readInt(true) match {
    //  case NodeSerializer.intNull => node.link = null
    //  case linkId =>
    //    val link = {
    //      try nodes(linkId)
    //      catch {
    //        case e: java.util.NoSuchElementException => Node.emptyNode
    //      }
    //    }
    //    link.uniqId = linkId
    //    nodes(linkId) = link

    //    node.link = link
    //}
    node.tids = input.readInt(true)

    node
  }
}

