package org.meerkat.graph.benchmark.neosemantics

import java.io.File
import java.util

import org.meerkat.Syntax.syn
import org.meerkat.graph.neo4j.Neo4jInput
import org.meerkat.graph.neo4j.Neo4jInput.Entity
import org.meerkat.parsers.Parsers._
import org.meerkat.parsers._
import org.neo4j.graphdb.factory.{GraphDatabaseFactory, GraphDatabaseSettings}

trait WithNeo4j extends App with SimpleBenchmark {
  def withGraph(pathToFile: String, pathToConf: String)(
      fun: (Neo4jInput => Unit)): Unit = {

    val dbFile = new File(pathToFile)
    val dbConf = new File(pathToConf)
    val graphDb = new GraphDatabaseFactory()
      .newEmbeddedDatabaseBuilder(dbFile)
      .loadPropertiesFromFile(dbConf.getAbsolutePath)
      .setConfig(GraphDatabaseSettings.allow_upgrade, "true")
      .newGraphDatabase()

    val tx    = graphDb.beginTx()
    val graph = new Neo4jInput(graphDb)
    try {
      fun(graph)
    } finally {
      tx.success()
      tx.close()
    }
  }

  def printPathLengthWithFinish(queryToDb: QueryToDb[Entity, Entity, String])(
      graph: Neo4jInput): Unit = {

    val testResult = benchmarkSample(graph,
                                     queryToDb.findPathLengthWithFinishQuery,
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

object RdfConstants {
  val RDFS__SUB_CLASS_OF       = "rdfs__subClassOf"
  val RDF__TYPE                = "rdf__type"
  val SKOS__BROADER_TRANSITIVE = "skos__broaderTransitive"
}

sealed trait QueryToDb[L, N, V] {
  def checkIfHas[T](e: Entity, prop: String)(p: T => Boolean): Boolean = {
    e.hasProperty(prop) && p(e.getProperty[T](prop))
  }

  def startVertexes: Symbol[L, Entity, Entity]
  def findFinishQuery: Symbol[L, N, V]
  def findPathsQuery: Symbol[L, N, util.ArrayDeque[V]]
  def findPathLengthQuery: Symbol[L, N, Int]
  def findPathLengthWithFinishQuery: Symbol[L, N, (Int, V)]

}

sealed trait RdfQuery extends QueryToDb[Entity, Entity, String] {
  protected val uriFromV: Symbol[Entity, Entity, String] =
    syn(V((_: Entity) => true) ^ ((_: Entity).getProperty[String]("uri")))

  override def findPathLengthWithFinishQuery
    : Symbol[Entity, Entity, (Int, String)] =
    syn(findPathLengthQuery ~ uriFromV & { case len ~ uri => (len, uri) })

  override def startVertexes: Symbol[Entity, Entity, Entity] =
    syn(V(checkIfHas[String](_: Entity, "uri")(_ => true)) ^^)
}

case class SameGeneration(private val edgeName: String) extends RdfQuery {
  protected val OUT: Edge[Entity] = outE((_: Entity).label() == edgeName)
  protected val IN: Edge[Entity]  = inE((_: Entity).label() == edgeName)

//  override def startVertexes: Symbol[Entity, Entity, Entity] =
//    syn(OUT ~ OUT ~ OUT ~ OUT ~ super.startVertexes ~ OUT &&)

  private def S: Symbol[Entity, Entity, _] = syn(IN ~ S ~ OUT | OUT)
  override def findFinishQuery: Symbol[Entity, Entity, String] =
    syn(
      S.?
      // without & {case ...} type error
        ~ uriFromV
        & { case _ ~ a => a }
    )

  override def findPathsQuery: Symbol[Entity, Entity, util.ArrayDeque[String]] =
    syn(
      syn(
        IN ~ uriFromV ~ findPathsQuery ~ OUT ~ uriFromV
          & {
            case (l: String) ~ (deque: util.ArrayDeque[String]) ~ (r: String) =>
              deque.addLast(r)
              deque.addFirst(l)
              deque
          }
          | OUT ~ uriFromV
            & {
              case uri: String =>
                val deque = new util.ArrayDeque[String]()
                deque.add(uri)
                deque
            }
      )
    )
  override def findPathLengthQuery: Symbol[Entity, Entity, Int] =
    syn(IN ~ findPathLengthQuery ~ OUT & {
      case inLen: Int => inLen + 2
    } | OUT ~ uriFromV & (_ => 1))

}

case object GPPerf1 extends QueryToDb[Entity, Entity, String] with RdfQuery {

  protected val SCO: Edge[Entity] = outE(
    (_: Entity).label() == RdfConstants.RDFS__SUB_CLASS_OF)
  protected val SCOR: Edge[Entity] = inE(
    (_: Entity).label() == RdfConstants.RDFS__SUB_CLASS_OF)

  protected val T: Edge[Entity] = outE(
    (_: Entity).label() == RdfConstants.RDF__TYPE)
  protected val TR: Edge[Entity] = inE(
    (_: Entity).label() == RdfConstants.RDF__TYPE)

  private def S: Symbol[Entity, Entity, _] =
    syn(SCOR ~ S ~ SCO | TR ~ S ~ T | SCOR ~ SCO | TR ~ T)

  override def findFinishQuery: Symbol[Entity, Entity, String] =
    syn(
      S.?
      // without & {case ...} type error
        ~ syn(V((_: Entity) => true) ^ ((_: Entity).getProperty[String]("uri")))
        & { case _ ~ a => a }
    )
  override def findPathsQuery: Symbol[Entity, Entity, util.ArrayDeque[String]] =
    ???

  override def findPathLengthQuery: Symbol[Entity, Entity, Int] =
    syn(
      SCOR ~ findPathLengthQuery ~ SCO & {
        case inLen: Int => inLen + 2
      }
        | TR ~ findPathLengthQuery ~ T & {
          case inLen: Int => inLen + 2
        }
        | SCOR ~ uriFromV ~ SCO & (_ => 2)
        | TR ~ uriFromV ~ T & (_ => 2)
    )
}
