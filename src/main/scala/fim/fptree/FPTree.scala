package fim.fptree

import fim.fptree._

import scala.annotation.tailrec

import scala.collection.immutable.Stack
import scala.collection.mutable.ListBuffer
import scala.collection.mutable.Set

object FPTree {
  

  @tailrec
  def fpGrowth(
      subTrees: scala.collection.mutable.Stack[FPTree],
      itemSets: ListBuffer[(Stack[Int],Int)]): ListBuffer[(Stack[Int],Int)] = {

    if (subTrees.isEmpty)
      return itemSets

    val tree = subTrees.pop()

    if (tree.root.count > tree.sup) {
      if (!tree.linearGraph) {

        if (!tree.itemSet.isEmpty)
          itemSets.append( (tree.itemSet, tree.root.count) )

        tree.table.foreach {case (_,c) =>

          var node = c
          var innerFreq = 0
          while (node != null) {
            innerFreq += node.count
            node = node.link
          }

          if (innerFreq > tree.sup) {
            val cfpTree = tree.projectTree(c)
            subTrees.push(cfpTree)
          }

        }

      } else {
        itemSets.appendAll(tree.powerSet)
        itemSets
      }
    }

    fpGrowth(subTrees, itemSets)
  }
}


case class FPTree(
    var root: Node = null,
    var freq: Map[Int, Int] = null,
    var sup: Int = 0,
    var mi: Int = 2,
    var rho: Int = 2,
    var itemSet: Stack[Int] = Stack[Int](),
    var table: scala.collection.mutable.Map[Int,Node] = scala.collection.mutable.Map[Int, Node]()) extends Serializable {

  def powerSet = {
    @tailrec
    def powerSetRec(node: Node, pSet: ListBuffer[(Stack[Int],Int)]): ListBuffer[(Stack[Int],Int)] = {
      if (node == null || node.count <= this.sup) {
        pSet
      } else {
        
        pSet.appendAll(
          pSet.map {case (p,_) => (p.push(node.itemId), node.count)}
        )

        //if (node.children.size == 1) // ATENÇÃO
        if (node.children.size == 1)
          powerSetRec(node.children.iterator.next, pSet)
        else
          powerSetRec(null, pSet)

        //pSet
      }
    }

    val pSet = ListBuffer( (this.itemSet, this.root.count) )
    if (this.root.children.size == 1)
      powerSetRec(this.root.children.iterator.next, pSet)
    pSet
  }

  def linearGraph = {
    @tailrec
    def linearGraphRec(node: Node): Boolean = node.children.size match {

      case 0 => true

      case 1 =>
        linearGraphRec(node.children.iterator.next) 

      case _ => false
    
    }

    linearGraphRec(this.root)
  }

  def buildTree(
      transIter: Iterator[Array[Int]],
      countIter: Iterator[Int] = Iterator.continually(1)) = {

    while (transIter.hasNext) {
      
      val trans = transIter.next
      val count = countIter.next
      var sortedTrans = trans

      // freq equals null means that we are projecting trees and therefore, the
      // transactions are already sorted.
      if (this.freq != null) {

        sortedTrans = trans.
        filter(t => this.freq(t) > this.sup).
        sortWith {(it1, it2) =>
          val cmp = (this.freq(it1) compare this.freq(it2))
          if (cmp == 0) it1 < it2
          else cmp > 0
        }
      }

      this.root.count += count
      this.root.insertTrans(sortedTrans, this.table, count)
    }
    this.table
  }

  //private def buildTreeRec(tree: Node,
  //    prefix: Stack[Int],
  //    subTree: Node): Unit = (prefix.head, prefix.tail) match {

  //  case (p, Stack()) => // insertion of subtree

  //    var nextNode = tree
  //    if (!tree.isRoot || !subTree.isRoot)
  //      nextNode = tree.insertTrans(
  //        Array(subTree.itemId), this.table, subTree.count)

  //    subTree.children.foreach {case (_,c) =>
  //      buildTreeRec(nextNode, prefix, c)
  //    }

  //  case (p, ps) => // insertion of prefix

  //    val nextNode = tree.insertTrans(Array(p), this.table, subTree.count)
  //    buildTreeRec(nextNode, ps, subTree)
  //}


  private def buildTreeRec(
      tree: Node,
      prefix: Stack[Int],
      subTree: Node): Unit = {
    buildTreeRec( scala.collection.mutable.Stack((tree, prefix, subTree)) )
  }


  @tailrec
  private def buildTreeRec(
      args: scala.collection.mutable.Stack[(Node,Stack[Int],Node)]): Unit = {

    if (args.isEmpty)
      return

    val (tree, prefix, subTree) = args.pop()

    (prefix.head, prefix.tail) match {
      
      case (p, Stack()) => // insertion of subtree

        var nextNode = tree
        if (!tree.isRoot || !subTree.isRoot)
          nextNode = tree.insertTrans(
            Array(subTree.itemId), this.table, subTree.count)

          subTree.children.foreach (c => args.push( (nextNode, prefix, c) ))

          buildTreeRec(args)

      case (p, ps) => // insertion of prefix

        val nextNode = tree.insertTrans(Array(p), this.table, subTree.count)
        buildTreeRec( args.push((nextNode, ps, subTree)) )
    }

  }

  def buildTreeFromChunks(chunksIter: Iterator[ (Stack[Int],Node) ]) = {

    while (chunksIter.hasNext) {
      val (prefix,subTree) = chunksIter.next
      buildTreeRec(this.root, prefix, subTree)
      this.root.count += subTree.count
    }
    this.table
    
  }

  def buildCfpTreesFromChunks(prefixCfpTrees: (Stack[Int], Iterable[Node])) = {
    
    val (prefix, cfpTrees) = prefixCfpTrees
    this.itemSet = prefix

    cfpTrees.foreach {subTree =>
      subTree.children.foreach {c =>
        buildTreeRec(this.root, Stack[Int](this.root.itemId), c)
      }
      this.root.count += subTree.count
    }
    this.table
  }

  def miTrees = {
    def miTreesRec(prefix: Stack[Int], node: Node,
        chunks: ListBuffer[(Stack[Int], Node)]): Stack[Int] = {

      val newPrefix = prefix :+ node.itemId

      if (node.level < this.mi && node.tids > 0) {

        val emptyNode = Node(node.itemId, node.tids, null, node.tids)
        emptyNode.count = node.tids
        chunks.append( (newPrefix, emptyNode) )

      } else if (node.level == this.mi) {
        
        chunks.append( (newPrefix, node) )

      }
      // recursion version
      //node.children.foreach {case (_,c) => miTreesRec(newPrefix, c, chunks)}

      newPrefix

    }

    val miChunks = ListBuffer[(Stack[Int], Node)]()
    if (this.mi <= 0)
      miChunks.append( (Stack(this.root.itemId), this.root) )
    else{
      // recursion version
      //this.root.children.foreach {case (_,c) => miTreesRec(Stack[Int](), c, miChunks)}

      // iterative version
      val nodes = scala.collection.mutable.Stack[(Stack[Int], Node)]()
      this.root.children.foreach (c => nodes.push( (Stack[Int](), c) ) )

      while (!nodes.isEmpty) {
        val (prefix, node) = nodes.pop()

        val newPrefix = miTreesRec(prefix, node, miChunks)

        node.children.foreach (c => nodes.push( (newPrefix, c) ))
      }
    }
    miChunks
  }

  def projectTree(node: Node) = {
    
    def makePath(leaf: Node): Array[Int] = {
      var path = scala.collection.mutable.Stack[Int]()
      var node = leaf
      while (!node.isRoot) {
        if (!node.parent.isRoot)
          path.push(node.parent.itemId)
        node = node.parent
      }
      path.toArray
    }

    val cfpTree = FPTree(Node.emptyNode, null, this.sup, this.mi,
      this.rho, this.itemSet.push(node.itemId))
    var newTransactions = scala.collection.mutable.Stack[Array[Int]]()
    var newCounts = scala.collection.mutable.Stack[Int]()
    var branch = node

    while (branch != null) {

      val path = makePath(branch)
      //if (!path.isEmpty) {
        newTransactions.push(path)
        newCounts.push(branch.count)
      //}
      branch = branch.link

    }

    cfpTree.buildTree(newTransactions.iterator, newCounts.iterator)
    cfpTree
  }

  

  def rhoTreesRec(prefix: Stack[Int], chunks: ListBuffer[(Stack[Int], Node)]): Unit = {
    def rhoTreesRecRec(
        prefix: Stack[Int],
        node: Node,
        chunks: ListBuffer[(Stack[Int], Node)]): Unit = {

      val cfpTree = this.projectTree(node)

      // -------------------------------------------
      if (prefix.size + 1 <= rho) {

        val emptyNode = Node(cfpTree.root.itemId, cfpTree.root.count, null, 0)
        chunks.append( (prefix, emptyNode) )
        cfpTree.rhoTreesRec(prefix, chunks)

      } else {

        chunks.append( (prefix, cfpTree.root) )

      }
    }

    this.table.foreach {case (_,c) =>
      // preppended (stack like)
      rhoTreesRecRec(prefix :+ c.itemId, c, chunks)
    }
  }

  def rhoTrees = {
    
    val rhoChunks = ListBuffer[(Stack[Int], Node)]()

    // checking rho's value
    if (this.rho > 0) 
      this.rhoTreesRec(Stack[Int](), rhoChunks)
    else
      rhoChunks.append( (Stack[Int](), this.root) )

    rhoChunks

  }
  
  def fpGrowth(
      itemSets: ListBuffer[(Stack[Int],Int)] = ListBuffer()): ListBuffer[(Stack[Int],Int)] = {
    val subTrees = scala.collection.mutable.Stack[FPTree](this)
    FPTree.fpGrowth(subTrees, itemSets)
  }

  override def toString = "FPTree :: " + this.itemSet + " freq = " + this.freq + "\n" + this.root
}
