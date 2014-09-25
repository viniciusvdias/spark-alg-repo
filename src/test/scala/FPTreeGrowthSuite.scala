
import org.scalatest.FunSuite

import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner

class FPTreeGrowthSuite extends FunSuite {
  
  import model._
  import util._
  import org.apache.spark.SparkContext
  import org.apache.spark.SparkContext._
  import org.apache.spark.SparkConf

  @transient var sc: SparkContext = _
  //val testFile1 = "input/test1.txt"
  //val testFile2 = "input/test2.txt"

  test("fp-tree growth test"){
    //sc = new SparkContext("local", "test")
    //val ret = fpgrowth.main(Array(testFile1, "2"))
    //assert("some boolean")
  }

}
