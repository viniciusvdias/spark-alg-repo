package br.ufmg.cs.lib.dmining.fim.eclat

import org.apache.spark.{SparkConf, SparkContext}
import org.scalatest.{BeforeAndAfterAll, FunSuite}

class EclatSuite extends FunSuite with BeforeAndAfterAll {

  private var transPath: String = _
  private var conf: SparkConf = _
  
  /** set up spark context */
  override def beforeAll: Unit = {
    
    val loader = classOf[EclatSuite].getClassLoader
    val url = loader.getResource("twig-input.txt")
    transPath = url.getPath

    val logPath = loader.getResource("eclat.log").toURI.toString

    conf = new SparkConf().
      setMaster ("local[*]").
      setAppName ("Eclat").
      set ("spark.adaptive.logpath", logPath)
    
    // override spark conf
    import org.apache.log4j.{Level, Logger}
    Logger.getLogger("org").setLevel(Level.ERROR)
    Logger.getLogger("akka").setLevel(Level.ERROR)
    Logger.getLogger("br").setLevel(Level.INFO)

  }

  /** stop spark context */
  override def afterAll: Unit = {
  }

  // TODO: compare results
  test("Eclat") {
    val params = Eclat.Params(inputFile = transPath, minSupport = 0.0001)
    Eclat.run (params, Option(conf))
  }
}
