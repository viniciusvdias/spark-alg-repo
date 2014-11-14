package fptree

import scala.annotation.tailrec

import scala.collection.immutable.Stack
import scala.collection.mutable.ListBuffer
import scala.collection.mutable.Set

import fptree._

case class FPTree(
    var root: Node,
    var freq: Map[Int, Int],
    var sup: Int,
    var mi: Int,
    var rho: Int,
    var itemSet: Stack[Int] = Stack[Int]()) extends Serializable {

  val table = scala.collection.mutable.Map[Int, Node]()

  def powerSet = {
    def powerSetRec(node: Node, pSet: ListBuffer[(Stack[Int],Int)]): ListBuffer[(Stack[Int],Int)] = {
      if (node.count <= this.sup)
        pSet
      else {
        
        pSet.appendAll(
          pSet.map {case (p,_) => (p.push(node.itemId), node.count)}
        )
        println("+++++- " + pSet + " (" + node.itemId + ")")

        if (node.children.size == 1)
          powerSetRec(node.children.values.iterator.next, pSet)

        pSet
      }
    }

    val pSet = ListBuffer( (this.itemSet, this.root.count) )
    println(" -----+ " + pSet)
    if (this.root.children.size == 1) {
      println(" aqui no if ")
      this.root.children.iterator.next match {
        case (_,c) => powerSetRec(c, pSet)
      }
    }
    pSet
  }

  def linearGraph = {
    @tailrec
    def linearGraphRec(node: Node): Boolean = node.children.size match {

      case 0 => true

      case 1 =>
        node.children.iterator.next match {
          case (_,c) => linearGraphRec(c)
        }

      case _ => false
    
    }

    linearGraphRec(this.root)
  }

  def buildTree(
      transIter: Iterator[Array[Int]],
      countIter: Iterator[Int] = Iterator.continually(1)) = {

    //println("build tree call")

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

      //println(sortedTrans.mkString(", "))
      this.root.count += count
      this.root.insertTrans(sortedTrans, this.table, count)
    }
    //println()
    this.table
  }

  def buildTreeRec(tree: Node,
      prefix: Stack[Int],
      subTree: Node): Unit = (prefix.head, prefix.tail) match {

    case (p, Stack()) => // insertion of subtree

     // println("prefix " + prefix + "\nsubTree ======== \n" + subTree)

      var nextNode = tree
      if (!tree.isRoot || !subTree.isRoot)
        nextNode = tree.insertTrans(
          Array(subTree.itemId), this.table, subTree.count)

      subTree.children.foreach {case (_,c) =>
        buildTreeRec(nextNode, prefix, c)
      }

    case (p, ps) => // insertion of prefix

      val nextNode = tree.insertTrans(Array(p), this.table, subTree.count)
      buildTreeRec(nextNode, ps, subTree)
  }

  def buildTreeFromChunks(chunksIter: Iterator[ (Stack[Int],Node) ]) = {

    //println("build tree from chunks call")

    while (chunksIter.hasNext) {
      val (prefix,subTree) = chunksIter.next
      //println("prefix = " + prefix.mkString(",") + "\n" + subTree)
      buildTreeRec(this.root, prefix, subTree)
      this.root.count += subTree.count
      //println("\n::::++ \n" + this.root)
    }
    //println()
    this.table
    
  }

  def buildCfpTreesFromChunks(prefixCfpTrees: (Stack[Int], Iterable[Node])) = {
    
    //println("build cfpTree from chunks call")

    val (prefix, cfpTrees) = prefixCfpTrees
    this.itemSet = prefix

    cfpTrees.foreach {subTree =>
      //println("prefix = " + prefix.mkString(",") + "\n" + subTree)
      subTree.children.foreach {case (_,c) =>
        buildTreeRec(this.root, Stack[Int](this.root.itemId), c)
      }
      this.root.count += subTree.count
    }
    //println()
    this.table
  }

  def miTrees = {
    def miTreesRec(prefix: Stack[Int], node: Node,
        chunks: ListBuffer[(Stack[Int], Node)]): Unit = {

      val newPrefix = prefix :+ node.itemId

      if (node.level < mi && node.tids > 0) {

        val emptyNode = Node(node.itemId, node.tids, null, node.tids)
        emptyNode.count = node.tids
        chunks.append( (newPrefix, emptyNode) )

      } else if (node.level == this.mi) {
        
        chunks.append( (newPrefix, node) )

      }
      node.children.foreach {case (_,c) => miTreesRec(newPrefix, c, chunks)}
    }

    val miChunks = ListBuffer[(Stack[Int], Node)]()
    if (this.mi <= 0)
      miChunks.append( (Stack(this.root.itemId), this.root) )
    else{
      this.root.children.foreach {case (_,c) => miTreesRec(Stack[Int](), c, miChunks)}
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
        println("path = " + path.mkString(","))
        newTransactions.push(path)
        newCounts.push(branch.count)
      //}
      branch = branch.link

    }

    cfpTree.buildTree(newTransactions.iterator, newCounts.iterator)
    cfpTree
  }

  def fpGrowth(
      itemSets: ListBuffer[(Stack[Int],Int)] = ListBuffer()): ListBuffer[(Stack[Int],Int)] = {

    if (this.root.count > this.sup) {
      if (!this.linearGraph) {

        if (!this.itemSet.isEmpty)
          itemSets.append( (this.itemSet, this.root.count) )

        this.table.foreach {case (_,c) =>

          var node = c
          var innerFreq = 0
          while (node != null) {
            innerFreq += node.count
            node = node.link
          }

          if (innerFreq > this.sup) {
            val cfpTree = this.projectTree(c)
            cfpTree.fpGrowth(itemSets)
          }

        }

      } else {
        itemSets.appendAll(this.powerSet)
      }
    }

    itemSets

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
  
  override def toString = "FPTree :: " + this.itemSet + " freq = " + this.freq + "\n" + this.root
}
