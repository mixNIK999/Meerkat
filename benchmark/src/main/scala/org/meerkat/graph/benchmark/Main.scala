package org.meerkat.graph.benchmark

import org.meerkat.graph.benchmark.neosemantics.{
  GPPerf1,
  QueryToDb,
  SimpleBenchmark,
  WithNeo4j
}
import org.meerkat.graph.neo4j.Neo4jInput.Entity

object Main extends App with WithNeo4j {
  withGraph(
    "/home/mishock/neo4j/data/databases/graph.db",
    "/home/mishock/neo4j/conf/neo4j.conf"
  ) { graph =>
    //  val queryToDb: QueryToDb[Entity, Entity, String] = SameGeneration(RdfConstants.RDFS__SUB_CLASS_OF)
    val queryToDb: QueryToDb[Entity, Entity, String] = GPPerf1

    val testResult = benchmarkSample(graph,
                                     queryToDb.findPathLengthQueryWithoutStart,
                                     2,
                                     queryToDb.startVertexes)

    testResult
      .drop(5)
      .map({ case (time, mem, res) => (time, mem, res.sum, res.length) })
      .take(4)
      .foreach({
        case (time, mem, sum, len) =>
          println(s"$time ms; $mem kB; sum len $sum; $len items")
      })
  }
}
