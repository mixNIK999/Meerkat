package org.meerkat.graph.neo4j.neosemantics

import java.io.File

import org.meerkat.Syntax.syn
import org.meerkat.graph.neo4j.Neo4jInput
import org.meerkat.graph.neo4j.Neo4jInput.Entity
import org.meerkat.parsers.Parsers._
import org.meerkat.parsers._
import org.neo4j.graphdb.factory.{GraphDatabaseFactory, GraphDatabaseSettings}
import org.scalameter.Measurer

object RdfNeo4jBenchmarkUsingNeosemantics extends App with SimpleBenchmark {
  val SUB_CLASS_OF       = "rdfs__subClassOf"
  val edgeSubClassOf     = outE((_: Entity).label() == SUB_CLASS_OF)
  val backEdgeSubClassOf = inE((_: Entity).label() == SUB_CLASS_OF)

  def checkIfHas[N](e: Entity, prop: String)(p: N => Boolean): Boolean = {
    e.hasProperty(prop) && p(e.getProperty[N](prop))
  }

  val dbFile = new File("/home/mishock/neo4j/data/databases/graph.db")
  val dbConf = new File("/home/mishock/neo4j/conf/neo4j.conf")
  val graphDb = new GraphDatabaseFactory()
    .newEmbeddedDatabaseBuilder(dbFile)
    .loadPropertiesFromFile(dbConf.getAbsolutePath)
    .setConfig(GraphDatabaseSettings.allow_upgrade, "true")
    .newGraphDatabase()

  val tx    = graphDb.beginTx()
  val graph = new Neo4jInput(graphDb)

  def sameGen: Symbol[Entity, Entity, _] =
    syn(backEdgeSubClassOf ~ sameGen.? ~ edgeSubClassOf)

  val pred = checkIfHas[String](_: Entity, "uri")(_.startsWith("node"))
  val queryWithoutStart = syn(
    sameGen.?
      ~ syn(V((_: Entity) => true) ^ (_.getProperty[String]("uri"))) & {
      case _ ~ a => a
    })
  val ans = executeQuery(syn(V(pred) ~ queryWithoutStart &&), graph).toList
  println(ans)

  val testResult = benchmarkSample(graph, queryWithoutStart, 1, pred).toList

  testResult.foreach(println(_))

  tx.success()
  tx.close()
}
