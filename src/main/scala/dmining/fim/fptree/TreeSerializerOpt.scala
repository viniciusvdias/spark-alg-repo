package dmining.fim.fptree

import com.esotericsoftware.kryo.io.Input
import com.esotericsoftware.kryo.io.Output
import com.esotericsoftware.kryo.Serializer
import org.apache.spark.serializer.KryoRegistrator
import com.esotericsoftware.kryo.Kryo
import scala.collection.mutable.Map

import org.apache.log4j.Logger
import org.apache.log4j.Level

// registration which avoids default recursive serialization
class TreeOptRegistrator extends KryoRegistrator {
  override def registerClasses(kryo: Kryo) =
    kryo.register(classOf[FPTree], new TreeSerializerOpt)
}

object TreeSerializerOpt {
  val TNodeType = (-127).toByte
  val RNodeType = (-126).toByte
  val Return = (-125).toByte
  val EndOfTree = (-124).toByte
}

class TreeSerializerOpt extends Serializer[FPTree] {
  
  override def write (k: Kryo, output: Output, tree: FPTree) = {
    
    def writeNode(output: Output, node: Node) = node match {
      case tNode: TNode =>
        output.writeByte(TreeSerializerOpt.TNodeType)
        output.writeInt(tNode.count)
        output.writeInt(tNode.itemId)
        output.writeInt(tNode.tids)

      case rNode: RNode =>
        output.writeByte(TreeSerializerOpt.RNodeType)
        output.writeInt(rNode.count)
        output.writeInt(rNode.itemId)
    }

    val stack = scala.collection.mutable.Stack[Node](tree.root)

    output.writeBoolean(!tree.linksTable.isEmpty)
    output.writeInt(tree.itemSet.size)
    tree.itemSet.foreach (output.writeInt)

    var ret = 0
    while (!stack.isEmpty) {
      stack.pop() match {
        case Node.Null => ret += 1
        case node =>
          // write node
          if (ret > 0) {
            output.writeByte(TreeSerializerOpt.Return)
            output.writeInt(ret)
            ret = 0
          }
          // write node, actually
          writeNode(output, node)

          stack.push(Node.Null)
          node.children.foreach(stack.push)
      }
    }
    output.writeByte(TreeSerializerOpt.EndOfTree)
  }

  override def read (kryo: Kryo, input: Input, t: Class[FPTree]): FPTree = {

    def readRNode(input: Input): Node = {
      val count = input.readInt()
      val itemId = input.readInt()
      RNode(itemId, count)
    }

    def readTNode(input: Input): Node = {
      val count = input.readInt()
      val itemId = input.readInt()
      val tids = input.readInt()
      val tNode = TNode(itemId, count)
      tNode.tids = tids
      tNode
    }

    // manage link
    def updateLinksTableOrNot(updateLinksTable: Boolean, node: Node, tree: FPTree) = updateLinksTable match {
      case false => // do nothing
      case true =>
        node.link = tree.linksTable.getOrElse(node.itemId, Node.Null)
        tree.linksTable(node.itemId) = node
    }

    // links table matters ?? Saving memory in the endpoint
    val updateLinksTable = input.readBoolean()
    
    // read itemSet
    val n = input.readInt()
    var itemSet = new Array[Int](n)
    var i = 0
    while (i < n) { itemSet(i) = input.readInt(); i += 1 }

    val root: Node = input.readByte() match {
      case TreeSerializerOpt.RNodeType => readRNode(input)
      case TreeSerializerOpt.TNodeType => readTNode(input)
    }

    val tree = FPTree(root, itemSet)

    var node = root
    var break = false
    while (!break) {
      input.readByte() match {

        case TreeSerializerOpt.EndOfTree => break = true

        case TreeSerializerOpt.Return =>
          // go up in the tree
          var ret = input.readInt()
          while (ret > 0) {
            node = node.parent
            ret -= 1
          }

        case TreeSerializerOpt.TNodeType =>
          val child = readTNode(input)
          node.addChild(child)
          updateLinksTableOrNot(updateLinksTable, child, tree)
          node = child

        case TreeSerializerOpt.RNodeType =>
          val child = readRNode(input)
          node.addChild(child)
          updateLinksTableOrNot(updateLinksTable, child, tree)
          node = child
      }
    }
    tree
  }

}

