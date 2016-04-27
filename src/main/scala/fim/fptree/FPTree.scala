package fim.fptree

import scala.annotation.tailrec

import scala.collection.mutable.{Stack, Queue,
  ArrayBuffer, WrappedArray, Map}

import org.apache.spark.Logging

import org.apache.log4j.Logger
import org.apache.log4j.Level

object FPTree {
  val emptyItemSet = Array.empty[Int]
  val log = Logger.getLogger(classOf[FPTree].getName)
  log.setLevel(Level.DEBUG)

  def muTrees(tree: FPTree, mu: Int) = {

    val muPairs = ArrayBuffer[(WrappedArray[Int],Any)]()
    // no rebalancing scheme was required, send the whole tree
    if (mu <= 0) muPairs.append( (Array.empty[Int], tree) )
    else {
      // split local tree into muTrees, with the purpose of rebalancing
      val nodes = Queue[(Array[Int],Node)]()
      tree.root.children.foreach { c => nodes.enqueue( (Array.empty[Int], c) ) }

      // insert dummy to keep track of the level
      nodes.enqueue(Node.Null)

      // iterative BFS traversal in the tree
      var level = 1 // root's children level
      while (!nodes.isEmpty) {
        nodes.dequeue() match {
          // every time one goes here, we have exhausted one level
          case Node.Null =>
            level += 1
            if (!nodes.isEmpty) nodes.enqueue(Node.Null) // level frontier

          // process node, keeping track of the prefix on the way down
          case (prefix, node) =>
            val newPrefix = prefix :+ node.itemId

            // need to check whether a transaction ends in this node (TNode)
            if (level < mu) {
              node match {
                case tNode: TNode => muPairs.append( (newPrefix, tNode.tids) )
                case _ => // do nothing
              }
              // enqueue next level nodes (which have level >= mu, for sure)
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

  def rhoTrees(tree: FPTree, rho: Int) = {
    def doProjection(tree: FPTree,
        node: Node,
        pairs: ArrayBuffer[(WrappedArray[Int],Any)]) = {

      val cfpTree = tree.projectTree(node)

      if (cfpTree.itemSet.size < rho) {
        pairs.append( (cfpTree.itemSet,cfpTree.root.count) )
        Some(cfpTree)
      } else {
        pairs.append( (cfpTree.itemSet,cfpTree) )
        None
      }
    }

    val rhoPairs = ArrayBuffer[(WrappedArray[Int], Any)]()
    // pre-projection is required ?
    if (rho <= 0) rhoPairs.append( (Array.empty[Int], tree) )
    else {
      val trees = Stack[FPTree](tree)
      while (!trees.isEmpty) {
        val nextTree = trees.pop()

        val linksIter = nextTree.linksTable.valuesIterator
        while (linksIter.hasNext) {
          val nextNode = linksIter.next
          doProjection(nextTree, nextNode, rhoPairs) match {
            case None => // do nothing, end of the line for projection
            case Some(cfpTree) => trees.push(cfpTree)
          }
        }
      }
    }
    rhoPairs.iterator
  }

  def fpGrowth(tree: FPTree, minSup: Int) = tree.root.count match {

    case count if count <= minSup => Iterator.empty
    case _ =>
      val itemSets = ArrayBuffer.empty[(Array[Int],Int)]

      val trees = Stack[FPTree](tree)
      while (!trees.isEmpty) {
        val nextTree = trees.pop()

        if (!nextTree.linearGraph) {

          if (!nextTree.itemSet.isEmpty)
            itemSets.append( (nextTree.itemSet, nextTree.root.count) )

          val linksIter = nextTree.linksTable.valuesIterator
          while (linksIter.hasNext) {
            val nextNode = linksIter.next

            var node = nextNode
            var innerFreq = 0
            while (node != null) {
              innerFreq += node.count
              node = node.link
            }

            if (innerFreq > minSup) {
              val cfpTree = nextTree.projectTree(nextNode)
              trees.push(cfpTree)
            }
          }
        } else itemSets.appendAll(nextTree.powerSet(minSup))
      }
      itemSets.iterator
  }
}

case class FPTree(var root: Node = Node.emptyRNode,
    var itemSet: Array[Int] = FPTree.emptyItemSet,
    var linksTable: Map[Int,Node] = Map[Int, Node]()) {

  var depth = 0
  def updateDepth (d: Int) = {depth = depth max d}
  var nNodes = 0
  def updateNnodes (n: Int) = {nNodes += n}

  def buildTree(transIter: Iterator[Array[Int]],
      countIter: Iterator[Int] = Iterator.continually(1)) = {

    while (transIter.hasNext) {
      val count = countIter.next
      this.root.count += count
      this.root.insertItems (transIter.next, this.linksTable, count,
        true, // do not update linksTable, they're not important right here
        false, // this is an original transaction, mark termination with TNode for mu-Mining
        this
      )
    }
  }
  
  @tailrec
  private def buildTreeRec(args: Stack[(Node,Node)]): Unit = args match {
    case Stack() => // do nothing
    case _ =>
      val (tree, subTree) = args.pop()
      val nextNode = tree.insertItems (Array(subTree.itemId),
        this.linksTable, subTree.count, tree=this)

      subTree.children.foreach (c => args.push( (nextNode, c) ))
      buildTreeRec(args)
  }

  def mergeRhoTree(subTree: Any) = subTree match {
    // merge *subTree* into this
    case FPTree(subTreeRoot,_,_) =>
      val args = Stack[(Node,Node)]()
      subTreeRoot.children.foreach (c => args.push( (this.root,c) ))
      buildTreeRec(args)
      this.root.count += subTreeRoot.count

    case count: Int => // could happens only if not called by *reduceByKey*
      this.root.count += count
  }

  def mergeMuTree(prefix: WrappedArray[Int], subTree: Any) = subTree match {
    // merge *prefix* + *subTree* into this
    // that involves (1) prefix insertion and (2) subTree merge
    case FPTree(subTreeRoot,_,_) =>
      val nextNode = this.root.insertItems (prefix, this.linksTable, subTreeRoot.count, tree=this)
      val args = Stack[(Node,Node)]()
      subTreeRoot.children.foreach (c => args.push( (nextNode,c) ))
      buildTreeRec(args)
      this.root.count += subTreeRoot.count

    // just increment prefix counter
    case count: Int =>
      this.root.insertItems (prefix, this.linksTable, count, tree=this)
      this.root.count += count
  }

  private def projectTree(node: Node) = {
    // using *List* as collection for two reasons:
    // (1) O(1) prepending
    // (2) object's life is supposed to be short (immutability)
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
      cfpTree.root.insertItems (path, cfpTree.linksTable, branch.count, tree=this)
      branch = branch.link
    }
    cfpTree
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

  override def toString = "FPTree :: " + this.itemSet.mkString(",") + "\n" + this.root +
    "\n" + "Table ::: " + this.linksTable.map {case (id,node) => id + 
      "->" + System.identityHashCode(node)}.mkString(" ")
}
