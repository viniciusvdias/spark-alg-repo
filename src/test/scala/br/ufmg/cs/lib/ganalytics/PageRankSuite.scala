package br.ufmg.cs.lib.ganalytics

import org.apache.spark.{SparkConf, SparkContext}
import org.scalatest.{BeforeAndAfterAll, FunSuite}

class PageRankSuite extends FunSuite with BeforeAndAfterAll {

  private var graphPath: String = _
  private var sc: SparkContext = _
  
  /** set up spark context */
  override def beforeAll: Unit = {
    
    val loader = classOf[PageRankSuite].getClassLoader
    val url = loader.getResource("citeseer-no-label.txt")
    graphPath = url.getPath

    val logPath = loader.getResource("pagerank.log").toURI.toString

    val conf = new SparkConf().
      setMaster ("local[*]").
      setAppName ("PageRank").
      set ("spark.adaptive.logpath", logPath)
    sc = new SparkContext(conf)
    
    // override spark conf
    import org.apache.log4j.{Level, Logger}
    Logger.getLogger("org").setLevel(Level.ERROR)
    Logger.getLogger("akka").setLevel(Level.ERROR)
    Logger.getLogger("br").setLevel(Level.INFO)

  }

  /** stop spark context */
  override def afterAll: Unit = {
    if (sc != null) {
      sc.stop()
    }
  }

  test("SparkPageRank") {
    val pgRunner = new SparkPageRank (graphPath, Array(10,10,10), 10, sc)
    pgRunner.run
  }
}
