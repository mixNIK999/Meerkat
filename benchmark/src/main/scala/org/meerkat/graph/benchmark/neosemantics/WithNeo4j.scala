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

  def printPathLengthWithStartAndFinish(
      testResult: Iterable[(String, (Double, Double, List[(Int, String)]))])
    : Unit = {
    testResult
      .map({
        case (startUri, (time, mem, res)) =>
          (startUri, time, mem, res.map(_._1).sum, res.length, res)
      })
      .foreach({
        case (startUri, time, mem, sum, len, res) =>
          print(s"$startUri $time $mem $sum $len ")
          res.foreach(p => print(s"$p "))
          println()
      })
  }

}

object RdfConstants {
  val RDFS__SUB_CLASS_OF       = "rdfs__subClassOf"
  val RDF__TYPE                = "rdf__type"
  val SKOS__BROADER_TRANSITIVE = "skos__broaderTransitive"
  val SKOS__NARROWER_TRANSITIVEY = "skos__narrowerTransitive"
  val OWL__ON_PROPERTY = "owl__onProperty"
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

object SameGenerationExample {

  val uriV: Symbol[Entity, Entity, Entity] =
    syn(V((_: Entity).hasProperty("uri")) ^^)

  def queryFromV[L, N](startV: Symbol[L, N, _],
                               query: Symbol[L, N, _]) =
    syn(startV ~ query ~ uriV &
        {case _ ~ (len:Int) ~ (v:Entity) =>
             (len, v.getProperty[String]("uri") )})

  def reduceChoice[L, N, V](xs: List[Symbol[L, N, V]]): Symbol[L, N, V] = {
    xs match {
      case x :: Nil => x
      case x :: y :: xs =>
        syn(xs.foldLeft(x | y)(_ | _))
    }
  }

  def sameGen[L, N, V](
      brs: List[(Symbol[L, N, V], Symbol[L, N, V])]): Symbol[L, N, Int] =
    reduceChoice(
      brs.map {
        case (lbr, rbr) =>
          syn((lbr ~ (sameGen(brs).?) ~ rbr) & {
            case _~((x:Int)::Nil)~_ =>  x + 2
            case _~Nil~_ => 2
          })
      }
    )
}
