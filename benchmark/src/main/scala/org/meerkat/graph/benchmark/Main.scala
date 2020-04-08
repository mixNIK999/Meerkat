package org.meerkat.graph.benchmark

import org.meerkat.Syntax.syn
import org.meerkat.graph.benchmark.neosemantics.RdfConstants._
import org.meerkat.graph.benchmark.neosemantics.SameGenerationExample._
import org.meerkat.graph.benchmark.neosemantics.WithNeo4j
import org.meerkat.graph.neo4j.Neo4jInput.Entity
import org.meerkat.input.Input
import org.meerkat.parsers.Parsers._
import org.meerkat.parsers.Parsers

object Main extends App with WithNeo4j {
  if (args.length < 3) {
    println("args.len < 3")
    System.exit(1)
  }
  //  Path to .db file
  //  Example: /home/user/neo4j/data/databases/graph.db
  val pathToFile = args(0)

  //  Path to neo4j.conf file
  //  Example /home/user/neo4j/data/databases/graph.db
  val pathToConf = args(1)

  // type (type_and_subClass, NT, BT ...)
  val experimentType = args(2)

  def runWithGraph =
    withGraph(
      pathToFile,
      pathToConf
    )(_)

  def runExample(brs: List[String]) =
    runWithGraph({ graph: Input[Entity, Entity] =>
      val symbolBrs = brs
        .map(name =>
          (syn(inE((_: Entity).label() == name) ^^), syn(outE((_: Entity).label() == name)^^)))
        .toList


      val result =
        benchmarkExample(graph, sameGen(symbolBrs), uriV)

      printPathLengthWithStartAndFinish(
        result
      )
    })

  experimentType match {

    case "type_and_subClass" => runExample(RDFS__SUB_CLASS_OF :: RDF__TYPE :: Nil)
    case "subClass" => runExample(RDFS__SUB_CLASS_OF :: Nil)
    case "NT" => runExample(SKOS__NARROWER_TRANSITIVEY :: Nil)
    case "BT" => runExample(SKOS__BROADER_TRANSITIVE :: Nil)
    case _ => throw new RuntimeException("unknown experiment type")
  }
}
