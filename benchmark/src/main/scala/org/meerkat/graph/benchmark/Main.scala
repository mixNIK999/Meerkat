package org.meerkat.graph.benchmark

import org.meerkat.graph.benchmark.neosemantics.{GPPerf1, QueryToDb, WithNeo4j}
import org.meerkat.graph.neo4j.Neo4jInput.Entity

object Main extends App with WithNeo4j {
//  val pathToFile = "/home/mishock/neo4j/data/databases/graph.db"
  val pathToFile = args(0)
//  val pathToConf = "/home/mishock/neo4j/conf/neo4j.conf"
  val pathToConf = args(1)

  withGraph(
    pathToFile,
    pathToConf
  ) { graph =>
    //  val queryToDb: QueryToDb[Entity, Entity, String] = SameGeneration(RdfConstants.RDFS__SUB_CLASS_OF)
    val queryToDb: QueryToDb[Entity, Entity, String] = GPPerf1

    val testResult = benchmarkSample(graph,
                                     queryToDb.findPathLengthWithFinishQuery,
                                     2,
                                     queryToDb.startVertexes)

    testResult
//      .take(5)
      .map({
        case (time, mem, res) => (time, mem, res.map(_._1).sum, res.length, res)
      })
      .foreach({
        case (time, mem, sum, len, res) =>
          print(s"$time $mem $sum $len ")
          res.foreach(p => print(s"$p "))
          println()
      })
  }
}
