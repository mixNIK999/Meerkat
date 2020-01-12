package org.meerkat.graph.neo4j.neosemantics

import java.io.File

import org.meerkat.Syntax.syn
import org.meerkat.graph.neo4j.Neo4jInput
import org.meerkat.graph.neo4j.Neo4jInput.Entity
import org.meerkat.graph.neo4j.neosemantics.GPPerf1.S
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

  val queryToDb: QueryToDb[Entity, Entity, String] = SameGeneration(RdfConstants.RDFS__SUB_CLASS_OF)


//  val ans = executeQuery(syn(queryToDb.startVertexes ~ queryToDb.queryWithoutStart & {case _ ~ b => b}), graph).toList
//  val ans = executeQuery(syn(queryToDb.startVertexes), graph).toList
//  println(ans)

  val testResult = benchmarkSample(graph, queryToDb.queryWithoutStart, 1, queryToDb.startVertexes)
  testResult.take(10).foreach({case (time, mem, res) => println(s"$time ms; $mem kB; ${res.length} items")})

  tx.success()
  tx.close()
}

object RdfConstants {
  val RDFS__SUB_CLASS_OF = "rdfs__subClassOf"
  val RDF__TYPE = "rdf__type"
  val SKOS__BROADER_TRANSITIVE = "dct__isPartOf"
}

sealed trait QueryToDb[L, N, V] {
  def checkIfHas[T](e: Entity, prop: String)(p: T => Boolean): Boolean = {
    e.hasProperty(prop) && p(e.getProperty[T](prop))
  }

  val startVertexes: Symbol[L, Entity, Entity] = syn(V(checkIfHas[N](_: Entity, "uri")(_ => true)) ^^)
  val queryWithoutStart: Symbol[L, N, V]
}
case class SameGeneration(private val edgeName: String) extends QueryToDb[Entity, Entity, String] {
  protected val OUT: Edge[Entity] = outE((_: Entity).label() == edgeName)
  protected val IN: Edge[Entity] = inE((_: Entity).label() == edgeName)

  private def S: Symbol[Entity, Entity, _] = syn(IN ~ S ~ OUT)
  override val queryWithoutStart: Symbol[Entity, Entity, String] = syn(
    S.?
      // without & {case ...} type error
      ~ syn(V((_: Entity) => true) ^ ((_: Entity).getProperty[String]("uri")))
      & { case _ ~ a => a }
  )

}

case object GPPerf1 extends QueryToDb[Entity, Entity, String] {

  protected val SCO: Edge[Entity] = outE((_: Entity).label() == RdfConstants.RDFS__SUB_CLASS_OF)
  protected val SCOR: Edge[Entity] = inE((_: Entity).label() == RdfConstants.RDFS__SUB_CLASS_OF)

  protected val T: Edge[Entity] = outE((_: Entity).label() == RdfConstants.RDF__TYPE)
  protected val TR: Edge[Entity] = inE((_: Entity).label() == RdfConstants.RDF__TYPE)

  private def S: Symbol[Entity, Entity, _] = syn(SCOR ~ S ~ SCO | TR ~ S ~ T | SCOR ~ SCO | TR ~ T)

  override val queryWithoutStart: Symbol[Entity, Entity, String] = syn(
    S.?
      // without & {case ...} type error
      ~ syn(V((_: Entity) => true) ^ ((_: Entity).getProperty[String]("uri")))
      & { case _ ~ a => a }
  )
}