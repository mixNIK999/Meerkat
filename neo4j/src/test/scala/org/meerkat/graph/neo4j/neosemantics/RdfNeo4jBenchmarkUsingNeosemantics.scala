package org.meerkat.graph.neo4j.neosemantics

import java.io.File

import org.meerkat.Syntax.syn
import org.meerkat.graph.neo4j.Neo4jInput
import org.meerkat.graph.neo4j.Neo4jInput.Entity
import org.meerkat.parsers.Parsers._
import org.meerkat.parsers._
import org.neo4j.graphdb.factory.GraphDatabaseSettings
import org.neo4j.graphdb.factory.GraphDatabaseFactory

object RdfNeo4jBenchmarkUsingNeosemantics extends App {
  val SUB_CLASS_OF       = "rdfs__subClassOf"
  val edgeSubClassOf     = outE((_: Entity).label() == SUB_CLASS_OF)
  val backEdgeSubClassOf = inE((_: Entity).label() == SUB_CLASS_OF)

  def checkIfHas[N](e: Entity, prop: String)(p: N => Boolean): Boolean = {
    e.hasProperty(prop) && p(e.getProperty[N](prop))
  }

  val dir = new File("/home/mishock/neo4j/data/databases/graph.db")
  val graphDb = new GraphDatabaseFactory()
    .newEmbeddedDatabaseBuilder(dir)
    .setConfig(GraphDatabaseSettings.allow_upgrade, "true")
    .newGraphDatabase()

  val tx    = graphDb.beginTx()
  val graph = new Neo4jInput(graphDb)

  def sameGen: Symbol[_, String, _] = syn(backEdgeSubClassOf ~ sameGen.? ~ edgeSubClassOf)

  val query = syn(
    V(checkIfHas(_: Entity, "uri")((_: String) == "node1dogn7h2dx31"))
      ~ sameGen.?
      ~ syn(V((_: Entity) => true) ^ (_.getProperty[String]("uri"))) & {
      case _ ~ a => a
    })
  val ans = executeQuery(query, graph).toList
  println(ans)

  tx.success()
  tx.close()
}
