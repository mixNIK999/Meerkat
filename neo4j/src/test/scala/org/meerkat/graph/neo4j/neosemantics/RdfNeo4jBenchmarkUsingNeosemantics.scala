package org.meerkat.graph.neo4j.neosemantics

import java.io.File

import org.meerkat.Syntax.syn
import org.meerkat.graph.neo4j.Neo4jInput
import org.meerkat.graph.neo4j.Neo4jInput.Entity
import org.meerkat.parsers.Parsers._
import org.meerkat.parsers._
import org.neo4j.graphdb.factory.{GraphDatabaseFactory, GraphDatabaseSettings}

object RdfNeo4jBenchmarkUsingNeosemantics extends App with SimpleBenchmark {

  val dbFile = new File("/home/mishock/neo4j/data/databases/graph.db")
  val dbConf = new File("/home/mishock/neo4j/conf/neo4j.conf")
  val graphDb = new GraphDatabaseFactory()
    .newEmbeddedDatabaseBuilder(dbFile)
    .loadPropertiesFromFile(dbConf.getAbsolutePath)
    .setConfig(GraphDatabaseSettings.allow_upgrade, "true")
    .newGraphDatabase()

  val tx    = graphDb.beginTx()
  val graph = new Neo4jInput(graphDb)

  val queryToDb: QueryToDb[Entity, Entity, String] = GPPerf2


  val ans = executeQuery(syn(queryToDb.startVertexes ~ queryToDb.queryWithoutStart & {case _ ~ b => b}), graph).toList
//  val ans = executeQuery(syn(queryToDb.startVertexes), graph).toList
  println(ans)

  val testResult = benchmarkSample(graph, queryToDb.queryWithoutStart, 1, queryToDb.startVertexes)
  testResult.take(50).foreach({case (time, mem, res) => println(s"$time ms; $mem kB; ${res.length} items")})

  tx.success()
  tx.close()
}


sealed trait QueryToDb[L, N, V] {
  def checkIfHas[T](e: Entity, prop: String)(p: T => Boolean): Boolean = {
    e.hasProperty(prop) && p(e.getProperty[T](prop))
  }

  val startVertexes: Symbol[L, N, Entity]
  val queryWithoutStart: Symbol[L, N, V]

  protected val SUB_CLASS_OF = "rdfs__subClassOf"
  protected val edgeSubClassOf: Edge[Entity] = outE((_: Entity).label() == SUB_CLASS_OF)
  protected val backEdgeSubClassOf: Edge[Entity] = inE((_: Entity).label() == SUB_CLASS_OF)
}
//case object GPPerf1 extends QueryToDb[Entity, Entity, String] {
//  override val startVertexes: Symbol[Entity, Entity, Entity] = syn(V(checkIfHas[Any](_: Entity, "uri")(_ => true)) ^^)
//
//  private val s = syn(s ~ )
//  override val queryWithoutStart: Symbol[Entity, Entity, String] = _
//}
case object GPPerf2 extends QueryToDb[Entity, Entity, String] {
  override val startVertexes: Symbol[Entity, Entity, Entity] =
    syn(syn(V((_:Entity).hasProperty("uri")) ^^))

  private def sameGen: Symbol[Entity, Entity, _] =
    syn(backEdgeSubClassOf ~ sameGen ~ edgeSubClassOf | edgeSubClassOf)

  override val queryWithoutStart: Symbol[Entity, Entity, String] = syn(
    // without ? not working ¯\_(ツ)_/¯
    sameGen.?
      ~ syn(V((_: Entity) => true) ^ ((_: Entity).getProperty[String]("uri"))) & {
      case _ ~ a => a
    })
}
