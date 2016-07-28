package br.ufmg.cs.util

import scala.collection.mutable.Map
import scala.collection.mutable.LinkedHashMap
import scala.collection.mutable.Set
import scala.reflect.ClassTag

class ParamsParser [Params : ClassTag](clientName: String) {
  
  val optMap = LinkedHashMap[String,(String,(String,Params)=>Params)]()
  val reqOpts = Set[String]()
  case class UsageException(msg: String) extends Exception(msg)

  def opt[Option](
      optName: String,
      text: String,
      parseFunc: (String,Params)=>Params,
      required: Boolean = false) = {

    optMap(optName) = (text, parseFunc)
    if (required) reqOpts += optName

  }

  def parse(args: Array[String], defaultParams: Params): Option[Params] = {
    try {
      if (args.size % 2 != 0)
        throw UsageException ("Option without corresponding value")

      val optNames = reqOpts
      var params = defaultParams
      
      for (i <- 0 until args.size by 2) {
        val optName = args(i).replaceAll ("--", "")

        if (args(i).startsWith ("--") && optMap.contains(optName)) {
          val optValue = args(i+1)
          val (text,func) = optMap(optName)
          params = func(optValue,params)
          reqOpts -= optName

        } else throw UsageException ("Invalid option.")
      }

      if (reqOpts.isEmpty) Option(params)
      else throw UsageException ("Missing required option.")
    } catch {
      case UsageException(msg) => showUsage(msg); None
    }
  }

  def showUsage(msg: String) = {

    val maxBlanks = optMap.keys.map (_.size).max + 1

    val optsUsage = optMap.map {
      case (opt,(text,_)) =>
        "\t--" + opt + (" "*(maxBlanks - opt.size)) + "<value>: " + text
    }.mkString ("\n")

    println(
s"""Issue: ${msg}
Options:
${optsUsage}
"""
    )
  }

}
