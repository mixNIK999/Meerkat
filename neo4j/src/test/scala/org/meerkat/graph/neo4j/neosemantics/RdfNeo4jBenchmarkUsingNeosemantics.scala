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

  val queryToDb: QueryToDb[Entity, Entity, String] = GPPerf1


//  val ans = executeQuery(syn(queryToDb.startVertexes ~ queryToDb.queryWithoutStart & {case _ ~ b => b}), graph).toList
//  val ans = executeQuery(syn(queryToDb.startVertexes), graph).toList
//  println(ans)

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

  protected val RDFS__SUB_CLASS_OF = "rdfs__subClassOf"
  protected val SCO: Edge[Entity] = outE((_: Entity).label() == RDFS__SUB_CLASS_OF)
  protected val SCOR: Edge[Entity] = inE((_: Entity).label() == RDFS__SUB_CLASS_OF)

  protected val RDF__TYPE = "rdf__type"
  protected val T: Edge[Entity] = outE((_: Entity).label() == RDF__TYPE)
  protected val TR: Edge[Entity] = inE((_: Entity).label() == RDF__TYPE)
}
case object GPPerf1 extends QueryToDb[Entity, Entity, String] {
  override val startVertexes: Symbol[Entity, Entity, Entity] = syn(V(checkIfHas[Any](_: Entity, "uri")(_ => true)) ^^)

  private def S: Symbol[Entity, Entity, _] = syn(S ~ SCOR ~ S ~ SCO | TR ~ S ~ T | SCOR ~ SCO | TR ~ T)

  override val queryWithoutStart: Symbol[Entity, Entity, String] = syn(
    S.?
      // without & {case ...} type error
      ~ syn(V((_: Entity) => true) ^ ((_: Entity).getProperty[String]("uri")))
      & { case _ ~ a => a }
  )
}
case object GPPerf2 extends QueryToDb[Entity, Entity, String] {
  override val startVertexes: Symbol[Entity, Entity, Entity] =
    syn(V((_:Entity).hasProperty("uri")) ^^)

  private def S: Symbol[Entity, Entity, _] =
    syn(SCOR ~ S ~ SCO | SCO)

  override val queryWithoutStart: Symbol[Entity, Entity, String] = syn(
    // without ? not working ¯\_(ツ)_/¯
    S.?
      ~ syn(V((_: Entity) => true) ^ ((_: Entity).getProperty[String]("uri")))
      & { case _ ~ a => a }
  )
}

case object Geo extends QueryToDb[Entity, Entity, String] {
  private val SKOS__BROADER_TRANSITIVE = "skos__broaderTransitive"
  private val BT: Edge[Entity] = outE((_: Entity).label() == SKOS__BROADER_TRANSITIVE)
  private val BTR: Edge[Entity] = inE((_: Entity).label() == SKOS__BROADER_TRANSITIVE)

  override val startVertexes: Symbol[Entity, Entity, Entity] =
//    syn(syn(V((_: Entity).hasProperty("uri")) ^^) ~ BT ~ BTR &&)
    syn(V((e:Entity) => e.hasProperty("uri") && e.uri == "http://lod.geospecies.org/orders/At8V4") ^^)

  private def S: Symbol[Entity, Entity, _] =
    syn(S ~ BT ~ S ~ BTR | BT ~ BTR)

  override val queryWithoutStart: Symbol[Entity, Entity, String] = syn(
    // without ? not working ¯\_(ツ)_/¯
    S
      ~ syn(V((_: Entity) => true) ^ ((_: Entity).getProperty[String]("uri")))
      & { case _ ~ a => a }
  )
}