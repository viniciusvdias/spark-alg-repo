package dmining.fim.fptree

import org.apache.spark.{SparkConf, SparkContext}
import org.scalatest.{BeforeAndAfterAll, FunSuite}

class TwiddSuite extends FunSuite with BeforeAndAfterAll {

  private var transPath: String = _
  private var conf: SparkConf = _
  
  /** set up spark context */
  override def beforeAll: Unit = {
    
    val loader = classOf[TwiddSuite].getClassLoader
    val url = loader.getResource("twig-input.txt")
    transPath = url.getPath

    val logPath = loader.getResource("twidd.log").toURI.toString

    conf = new SparkConf().
      setMaster ("local[*]").
      setAppName ("Twidd").
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
  test("Twidd") {
    val params = Twidd.Params(inputFile = transPath, minSupport = 0.0001)
    Twidd.run (params, Option(conf))
  }
}
