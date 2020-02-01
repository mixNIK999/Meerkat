package org.meerkat.graph.benchmark

import org.meerkat.graph.benchmark.neosemantics.{
  GPPerf1,
  RdfConstants,
  SameGeneration,
  WithNeo4j
}

object Main extends App with WithNeo4j {
  if (args.length < 3) {
    println("args.len < 3")
    System.exit(1)
  }
  //  val pathToFile = "/home/mishock/neo4j/data/databases/graph.db"
  val pathToFile = args(0)
//  val pathToConf = "/home/mishock/neo4j/conf/neo4j.conf"
  val pathToConf = args(1)

  val experimentType = args(2)

  def runWithGraph =
    withGraph(
      pathToFile,
      pathToConf
    )(_)

  experimentType match {
    case "GPPerf1" => runWithGraph(printPathLengthWithFinish(GPPerf1))
    case "GPPerf2" =>
      runWithGraph(
        printPathLengthWithFinish(
          SameGeneration(RdfConstants.RDFS__SUB_CLASS_OF)))
    case "GoProp" =>
      runWithGraph(printPathLengthWithFinish(SameGeneration("owl__onProperty")))
    case "enzime" =>
      runWithGraph(
        printPathLengthWithFinish(SameGeneration("skos__narrowerTransitive")))
    case "Geo" =>
      ??? //runWithGraph(printPathLengthWithFinish(SameGeneration(RdfConstants.RDFS__SUB_CLASS_OF)))
    case _ => new RuntimeException("unknown experiment type")
  }
}
