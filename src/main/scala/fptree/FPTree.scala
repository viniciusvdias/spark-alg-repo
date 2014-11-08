package fptree

import scala.collection.immutable.Stack
import scala.collection.mutable.ListBuffer

case class FPTree(
    var root: Node,
    var freq: Map[String, Int],
    var sup: Int,
    var mi: Int,
    var rho: Int,
    var itemSet: Stack[String] = Stack[String]()) extends Serializable {

  val table = scala.collection.mutable.Map[String, Node]()

  def buildTree(
      transIter: Iterator[Array[String]],
      countIter: Iterator[Int] = Iterator.continually(1)) = {

    println("build tree call")

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

      println(sortedTrans.mkString(", "))
      this.root.count += count
      this.root.insertTrans(sortedTrans, this.table, count)
    }
    println()
    this.table
  }

  def buildTreeRec(tree: Node,
      prefix: Stack[String],
      subTree: Node): Unit = (prefix.head, prefix.tail) match {

    case (p, Stack()) => // insertion of subtree

      val nextNode = tree.insertTrans(
        Array(subTree.itemId), this.table, subTree.count)

      subTree.children.foreach {case (_,c) =>
        buildTreeRec(nextNode, prefix, c)
      }

    case (p, ps) => // insertion of prefix

      val nextNode = tree.insertTrans(Array(p), this.table, subTree.count)
      buildTreeRec(nextNode, ps, subTree)
  }

  def buildTreeFromChunks(chunksIter: Iterator[ (Stack[String],Node) ]) = {

    println("build tree from chunks call")

    while (chunksIter.hasNext) {
      val (prefix,subTree) = chunksIter.next
      println("prefix = " + prefix.mkString(",") + "\n" + subTree)
      buildTreeRec(this.root, prefix, subTree)
      this.root.count += subTree.count
      println("\n::::++ \n" + this.root)
    }
    println()
    this.table
    
  }

  def buildCfpTreesFromChunks(prefixCfpTrees: (Stack[String], Iterable[Node])) = {
    
    println("build cfpTree from chunks call")

    val (prefix, cfpTrees) = prefixCfpTrees
    this.itemSet = prefix

    cfpTrees.foreach {subTree =>
      println("prefix = " + prefix.mkString(",") + "\n" + subTree)
      subTree.children.foreach {case (_,c) =>
        buildTreeRec(this.root, Stack[String](this.root.itemId), c)
      }
      this.root.count += subTree.count
    }
    println()
    this.table
  }

  def miTrees = {
    def miTreesRec(prefix: Stack[String], node: Node,
        chunks: ListBuffer[(Stack[String], Node)]): Unit = {

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
    
    val miChunks = ListBuffer[(Stack[String], Node)]()
    this.root.children.foreach {case (_,c) => miTreesRec(Stack[String](), c, miChunks)}
    miChunks
  }

  def rhoTreesRec(prefix: Stack[String], chunks: ListBuffer[(Stack[String], Node)]): Unit = {
    def rhoTreesRecRec(prefix: Stack[String], node: Node,
      chunks: ListBuffer[(Stack[String], Node)]): Unit = {

      def makePath(leaf: Node): Array[String] = {
        var path = scala.collection.mutable.Stack[String]()
        var node = leaf
        while (!node.isRoot) {
          if (!node.parent.isRoot)
            path.push(node.parent.itemId)
          node = node.parent
        }
        path.toArray
      }

      val cfpTree = FPTree(Node.emptyNode, null, this.sup, this.mi, this.rho)
      var newTransactions = scala.collection.mutable.Stack[Array[String]]()
      var newCounts = scala.collection.mutable.Stack[Int]()
      var branch = node

      while (branch != null) {

        newTransactions.push(makePath(branch))
        newCounts.push(branch.count)
        branch = branch.link

      }

      cfpTree.buildTree(newTransactions.iterator, newCounts.iterator)

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
    
    val rhoChunks = ListBuffer[(Stack[String], Node)]()

    // checking rho's value
    if (this.rho > 0) 
      this.rhoTreesRec(Stack[String](), rhoChunks)
    else
      rhoChunks.append( (Stack[String](), this.root) )

    rhoChunks

  }
  
  override def toString = "FPTree :: " + this.itemSet + " freq = " + this.freq + "\n" + this.root
}
