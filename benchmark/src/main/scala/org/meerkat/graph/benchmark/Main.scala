package org.meerkat.graph.benchmark

import org.meerkat.graph.benchmark.neosemantics.{GPPerf1, RdfConstants, RdfQuery, SameGeneration, WithNeo4j}
import org.meerkat.graph.neo4j.Neo4jInput.Entity
import org.meerkat.input.Input
import org.meerkat.parsers.Parsers._
import org.meerkat.parsers.Parsers
import org.meerkat.parsers._
import org.meerkat.Syntax.syn
import org.meerkat.graph.benchmark.neosemantics.RdfConstants._
import org.meerkat.graph.benchmark.neosemantics.SameGenerationExample._
import org.neo4j.cypher.internal.v3_4.expressions.True

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

  def runQTB(tp: RdfQuery) =
    runWithGraph(
      { graph =>
        printPathLengthWithStartAndFinish(
          benchmarkQueryToDb(SameGeneration(RDFS__SUB_CLASS_OF))(
            graph))
      }
    )

  def runExample2(brs: List[String]) =
    runWithGraph({ graph: Input[Entity, Entity] =>
      val symbolBrs = brs
        .map(name =>
          (syn(inE((_: Entity).label() == name) ^^), syn(outE((_: Entity).label() == name)^^)))
        .toList

      val q = queryFromV(syn(V(getIdFromNode(_: Entity) == 1)^^),
                         sameGen(symbolBrs))

      val result = executeQuery(q, graph).toList


      print(result.map(_._1).sum)
    })

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

  def runExample1(brs: List[String]) =
    runWithGraph({ graph: Input[Entity, Entity] =>
      val symbolBrs = brs
        .map(name =>
          (syn(inE((_: Entity).label() == name) ^^), syn(outE((_: Entity).label() == name)^^)))
        .toList


      val result =
        benchmarkExample1(graph, sameGen2(), uriV)

      print(result.take(100).toList)
    })

  experimentType match {
    case "GPPerf1" => runQTB(GPPerf1)
    case "GPPerf2" => runQTB(SameGeneration(RDFS__SUB_CLASS_OF))
    case "GoProp"  => runQTB(SameGeneration(OWL__ON_PROPERTY))
    case "enzime"  => runQTB(SameGeneration(SKOS__NARROWER_TRANSITIVEY))
    case "GeoBorderTr" =>
      runQTB(SameGeneration(SKOS__BROADER_TRANSITIVE))

    case "type_and_subClass" => runExample(RDFS__SUB_CLASS_OF :: RDF__TYPE :: Nil)
    case "subClass" => runExample(RDFS__SUB_CLASS_OF :: Nil)
    case "NT" => runExample(SKOS__NARROWER_TRANSITIVEY :: Nil)
    case "BT" => runExample(SKOS__BROADER_TRANSITIVE :: Nil)

    case "example" => runExample(RDFS__SUB_CLASS_OF :: Nil)
    case _ => throw new RuntimeException("unknown experiment type")
  }
}
