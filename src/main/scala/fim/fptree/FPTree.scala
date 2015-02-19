package fim.fptree

import fim.fptree._

import scala.annotation.tailrec

import scala.collection.mutable.Stack
import scala.collection.mutable.Queue
import scala.collection.mutable.ArrayBuffer
import scala.collection.mutable.WrappedArray

import org.apache.log4j.Logger
import org.apache.log4j.Level

object FPTree {

  val emptyItemSet = Array.empty[Int]
  val log = Logger.getLogger(classOf[FPTree].getName)
  log.setLevel(Level.DEBUG)
  
}

case class FPTree(
    var root: Node = Node.emptyRNode,
    var itemSet: Array[Int] = FPTree.emptyItemSet,
    var linksTable: scala.collection.mutable.Map[Int,Node] = scala.collection.mutable.Map[Int, Node]()) {

  def buildTree(transIter: Iterator[Array[Int]],
      countIter: Iterator[Int] = Iterator.continually(1)) = {

    while (transIter.hasNext) {
      
      val trans = transIter.next
      val count = countIter.next
      this.root.count += count
      this.root.insertItems (trans, this.linksTable, count,
        false, // do not update linksTable, they're not important at that moment
        true // this is an original transaction, mark termination with TNode for mu-Mining
      )
    }
    this.linksTable
  }
  
  @tailrec
  private def buildTreeRec(args: Stack[(Node,Node)]): Unit = args match {
    case Stack() => // do nothing
    case _ =>
      val (tree, subTree) = args.pop()
      val nextNode = tree.insertItems (Array(subTree.itemId),
        this.linksTable, subTree.count)

      subTree.children.foreach (c => args.push( (nextNode, c) ))
      buildTreeRec(args)
  }

  def mergeRhoTree(subTree: Any) = subTree match {
    
    case FPTree(subTreeRoot,_,_) =>
      val args = Stack[(Node,Node)]()
      subTreeRoot.children.foreach (c => args.push( (this.root,c) ))
      buildTreeRec(args)
      this.root.count += subTreeRoot.count

    // could happens only if not called by *reduceByKey*
    case count: Int =>
      this.root.count += count
  }

  def mergeMuTree(prefix: WrappedArray[Int], subTree: Any) = subTree match {

    case FPTree(subTreeRoot,_,_) =>
      val nextNode = this.root.insertItems (prefix.toArray, this.linksTable, subTreeRoot.count)
      val args = Stack[(Node,Node)]()
      subTreeRoot.children.foreach (c => args.push( (nextNode,c) ))
      buildTreeRec(args)
      this.root.count += subTreeRoot.count

    case count: Int =>
      this.root.insertItems (prefix.toArray, this.linksTable, count)
      this.root.count += count
  }

  def muTrees(mu: Int) = {

    val muPairs = ArrayBuffer[(WrappedArray[Int], Any)]()
    // no rebalancing scheme was required, send the whole tree
    if (mu <= 0) muPairs.append( (Array.empty[Int], this) )
    else {
      // split local tree into muTrees, with the purpose of rebalancing
      val nodes = Queue[(Array[Int], Node)]()
      this.root.children.foreach { c => nodes.enqueue( (Array.empty[Int], c) ) }

      // insert dummy to keep track of the level
      nodes.enqueue(Node.Null)

      // iterative BFS traversal in the tree
      var level = 1 // root's children level
      while (!nodes.isEmpty) {
        nodes.dequeue() match {

          case Node.Null =>
            level += 1
            if (!nodes.isEmpty) nodes.enqueue(Node.Null) // level frontier

          case (prefix, node) =>
            val newPrefix = prefix :+ node.itemId

            if (level < mu) {
              node match {
                case tNode: TNode => muPairs.append( (newPrefix, tNode.tids) )
                case _ => // do nothing
              }

              // enqueue next level nodes
              node.children.foreach {c => nodes.enqueue( (newPrefix, c) )}

            } else if (level == mu) {
              // mu level reached, send remaining tree
              node.parent = Node.Null // sanity: detach from original tree
              node.itemId = Node.emptyItemId
              val newTree = FPTree(node)
              
              muPairs.append( (newPrefix, newTree) )

            }
        }
      }
    }
    muPairs.iterator
  }

  private def projectTree(node: Node) = {

    def makePath(leaf: Node) = {
      @tailrec
      def makePathRec(node: Node, acc: List[Int]): List[Int] = node match {
        case _ if node.parent.isRoot => acc
        case _ => makePathRec(node.parent, node.parent.itemId :: acc)
      }
      makePathRec(leaf, Nil)
    }
    
    val cfpTree = FPTree(Node.emptyRNode, this.itemSet :+ node.itemId)
    
    // build conditioned transactions related to this node
    var branch = node
    while (branch != null) {
      val path = makePath(branch)
      cfpTree.root.count += branch.count
      cfpTree.root.insertItems (path.toArray, cfpTree.linksTable, branch.count)
      branch = branch.link
    }
    cfpTree
  }

  def rhoTrees(rho: Int) = {
    def doProjection(tree: FPTree,
        prefixNode: (Array[Int],Node),
        pairs: ArrayBuffer[(WrappedArray[Int],Any)]): FPTree = {

      val (prefix,node) = prefixNode
      val cfpTree = tree.projectTree(node)

      if (prefix.size < rho) {
        pairs.append( (prefix,cfpTree.root.count) )
        cfpTree

      } else {
        pairs.append( (prefix,cfpTree) )
        null

      }
    }

    val rhoPairs = ArrayBuffer[(WrappedArray[Int], Any)]()

    // pre-projection is required ?
    if (rho <= 0) rhoPairs.append( (Array.empty[Int], this) )
    else {

      val trees = Stack[FPTree](this)
      while (!trees.isEmpty) {

        val tree = trees.pop()
        val linksIter = tree.linksTable.valuesIterator
        while (linksIter.hasNext) {
          val nextNode = linksIter.next
          doProjection(tree, (tree.itemSet :+ nextNode.itemId, nextNode), rhoPairs) match {
            case null => // do nothing, end of the line for projection
            case cfpTree => trees.push(cfpTree)
          }
        }
      }
    }
    rhoPairs.iterator
  }

  private def linearGraph = {
    @tailrec
    def linearGraphRec(node: Node): Boolean = node.children.size match {
      case 0 => true
      case 1 => linearGraphRec(node.children.head) 
      case _ => false
    }
    linearGraphRec(this.root)
  }

  private def powerSet(minSup: Int) = {
    @tailrec
    def powerSetRec(node: Node, pSet: ArrayBuffer[(Array[Int],Int)]): ArrayBuffer[(Array[Int],Int)] = {
      if (node.count <= minSup) pSet
      else {
        
        pSet.appendAll(
          pSet.map {case (p,_) => (p :+ node.itemId, node.count)}
        )

        if (node.children.size == 1) powerSetRec(node.children.head, pSet)
        else pSet
      }
    }

    val pSet = ArrayBuffer( (this.itemSet, this.root.count) )
    if (this.root.children.size == 1)
      powerSetRec(this.root.children.head, pSet)
    pSet
  }

  def fpGrowth(minSup: Int) = {
    @tailrec
    def fpGrowthRec(subTrees: Stack[FPTree],
        itemSets: ArrayBuffer[(Array[Int],Int)]): ArrayBuffer[(Array[Int],Int)] = subTrees match {
      
      case Stack() => itemSets
      case _ =>
        val tree = subTrees.pop()
        if (tree.root.count > minSup) {
          if (!tree.linearGraph) {

            if (!tree.itemSet.isEmpty)
              itemSets.append( (tree.itemSet, tree.root.count) )

            val linksIter = tree.linksTable.valuesIterator
            while (linksIter.hasNext) {
              val nextNode = linksIter.next

              var node = nextNode
              var innerFreq = 0
              while (node != null) {
                innerFreq += node.count
                node = node.link
              }

              if (innerFreq > minSup) {
                val cfpTree = tree.projectTree(nextNode)
                subTrees.push(cfpTree)
              }
            }
          } else itemSets.appendAll(tree.powerSet(minSup))
        }
        fpGrowthRec(subTrees, itemSets)
    }

    val subTrees = Stack[FPTree](this)
    fpGrowthRec(subTrees, ArrayBuffer())
  }

  override def toString = "FPTree :: " + this.itemSet.mkString(",") + "\n" + this.root +
    "\n" + "Table ::: " + this.linksTable.map {case (id,node) => id + 
      "->" + System.identityHashCode(node)}.mkString(" ")
}
