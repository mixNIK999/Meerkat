package org.meerkat.graph.benchmark

import org.meerkat.graph.benchmark.neosemantics.{
  GPPerf1,
  RdfConstants,
  RdfQuery,
  SameGeneration,
  WithNeo4j
}
import org.meerkat.graph.neo4j.Neo4jInput.Entity
import org.meerkat.input.Input
import org.meerkat.parsers.Parsers._
import org.meerkat.parsers.Parsers
import org.meerkat.parsers._
import org.meerkat.Syntax.syn
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
          benchmarkQueryToDb(SameGeneration(RdfConstants.RDFS__SUB_CLASS_OF))(
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


      print(result.map(_._2).sum)
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
    case "GPPerf2" => runQTB(SameGeneration(RdfConstants.RDFS__SUB_CLASS_OF))
    case "GoProp"  => runQTB(SameGeneration("owl__onProperty"))
    case "enzime"  => runQTB(SameGeneration("skos__narrowerTransitive"))
    case "GeoBorderTr" =>
      runQTB(SameGeneration(RdfConstants.SKOS__BROADER_TRANSITIVE))
    case "example" =>
      runExample2(RdfConstants.RDFS__SUB_CLASS_OF :: Nil)
    case _ => throw new RuntimeException("unknown experiment type")
  }
}
