
import org.scalatest.FunSuite

import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner

import org.apache.log4j.Logger
import org.apache.log4j.Level

class fpgrowthSuite extends FunSuite {
  
  import fim.fptree._
  import fim.serialization._
  import org.apache.spark.SparkContext
  import org.apache.spark.SparkContext._
  import org.apache.spark.SparkConf

  // java default streams
  import java.io.FileOutputStream
  import java.io.FileInputStream

  // scala collections
  import scala.collection.mutable.ArrayBuffer

  // kryo serializer stuff
  import com.esotericsoftware.kryo.Kryo
  import org.apache.spark.serializer.KryoRegistrator
  import com.esotericsoftware.kryo.io.Input
  import com.esotericsoftware.kryo.io.Output
  import com.esotericsoftware.kryo.Serializer

  Logger.getLogger("org").setLevel(Level.OFF)
  Logger.getLogger("akka").setLevel(Level.OFF)

  def conf = new SparkConf()
    .setMaster("local")
    .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    .set("spark.kryo.registrator", "fim.serialization.SerialRegistrator")
    .setAppName("FP-Growth test")
  var sc: SparkContext = _
  val testFile1 = "input/test_artigo.txt"
   
  // defaults
  val sep = " "
  val numberOfPartitions = 2
  val _sup = 0.1
  var sup = _sup.toInt
  val mi = 2
  val rho = 2

  test("node serializer test"){
  
    val kryo = new Kryo()
    kryo.register(classOf[Node], new NodeSerializer())

    val output = new Output(new FileOutputStream("file.bin"))
    val outputNode = Node.emptyNode
    outputNode.addChild(Node(2,4,outputNode,5))
    outputNode.addChild(Node(3,4,outputNode,5))
    val outputTree = FPTree()
    outputTree.root = outputNode
    outputTree.table(0) = outputNode
    outputTree.table(1) = outputNode.children.iterator.next._2
    kryo.writeObject(output, outputTree)
    output.close()
    
    val input = new Input(new FileInputStream("file.bin"))
    //val inputTree = kryo.readObject(input, classOf[FPTree])
    //info("tree \n" + inputTree + "end.")
    input.close()
  }

  //test("node serializer test 2") {
  //  sc = new SparkContext(conf)

  //  val linesRDD = sc.textFile(testFile1)

  //  val transactionsRDD = linesRDD.
  //  map(l => (l split sep).map(it => it.toInt)).
  //  repartition(numberOfPartitions)

  //  val frequencyRDD = transactionsRDD.flatMap(trans => trans).
  //  map(it => (it, 1)).
  //  reduceByKey(_ + _)
  //  
  //  val transCount = transactionsRDD.count.toInt
  //  if (sup > 0)
  //    sup = (_sup * transCount).toInt
  //  else
  //    sup = (-1 * _sup).toInt

  //  // broadcast variables
  //  val frequencyBcast = sc.broadcast(frequencyRDD.collect.toMap)

  //  val supBcast = sc.broadcast(sup)
  //  val miBcast = sc.broadcast(mi)
  //  val rhoBcast = sc.broadcast(rho)
  //  val nTransBcast = sc.broadcast(transCount)

  //  // here we have local trees
  //  val localTreesRDD = transactionsRDD.mapPartitions {transIter =>
  //    val tree = FPTree(Node.emptyNode, frequencyBcast.value,
  //      supBcast.value, miBcast.value, rhoBcast.value)
  //    tree.buildTree(transIter)
  //    Iterator(tree.root)
  //  }.repartition(3)

  //  info("count = " + localTreesRDD.count)
  //  
  //  sc.stop()
  //  sc = null
  //  System.clearProperty("spark.driver.port")
  //}

}
