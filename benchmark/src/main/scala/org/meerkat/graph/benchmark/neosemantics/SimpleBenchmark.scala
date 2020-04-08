package org.meerkat.graph.benchmark.neosemantics

import org.meerkat.Syntax.syn
import org.meerkat.graph.benchmark.neosemantics.SameGenerationExample.queryFromV
import org.meerkat.graph.neo4j.Neo4jInput.Entity
import org.meerkat.input.Input
import org.meerkat.parsers.Parsers._
import org.meerkat.parsers._
import org.meerkat.sppf.NonPackedNode
import org.neo4j.graphdb.Node
import org.scalameter
import org.scalameter.{Key, Measurer, config}

trait SimpleBenchmark {

  def benchmarkQueryWith[L, N, T <: NonPackedNode, V](
                                                       graph: Input[L, N],
                                                       query: AbstractCPSParsers.AbstractSymbol[L, N, T, V],
                                                       runsNumber: Int,
                                                       measurer: Measurer[Double]): Double = {

    val list = for (i <- 0 until runsNumber) yield {
      val res = config(
        Key.exec.benchRuns -> 1
      ) withMeasurer {
        measurer
      } measure {
        executeQuery(query, graph).toList
      }
      query.reset()
      res
    }
    list.map(_.value).sum / runsNumber
  }

  def completeBenchmark[V](fun: () => V): (Double, Double, V) = {
    var time = 0.0
    var result: Option[V] = None
    val mem = (config(
      Key.exec.benchRuns -> 1
    ) withMeasurer {
      new Measurer.MemoryFootprint
    } measure {
      time = (config(
        Key.exec.benchRuns -> 1
      ) withMeasurer {
        new Measurer.Default
      } measure {
        result = Some(fun())
      }).value
    }).value
    (time, mem, result.get)
  }

  def getIdFromNode(e: Entity): Long = {
    e.entity.asInstanceOf[Node].getId
  }

  def benchmarkQuery[L, N, T <: NonPackedNode, V](
                                                   graph: Input[L, N],
                                                   query: AbstractCPSParsers.AbstractSymbol[L, N, T, V])
  : (Double, Double, List[V]) = {
    val ans = completeBenchmark(() => executeQuery(query, graph).toList)
    query.reset()
    ans
  }

  def forEachVertex[L, N, V](
                              graph: Input[L, N],
                              getStartVertex: Symbol[L, N, Entity])(fun: (String, Long) => V) = {
    val allVertex = executeQuery(getStartVertex, graph).map((v: Entity) =>
      (v.getProperty[String]("uri"), getIdFromNode(v)))
    allVertex.view.map { case (uri, id) => fun(uri, id) }
  }

  // time in ms, memory in kB
  def benchmarkExample[L, N <: Entity, QV](graph: Input[L, N],
                                           query: Symbol[L, N, QV],
                                           getStartVertex: Symbol[L, N, Entity]) = {

    forEachVertex(graph, getStartVertex) { (uri, id) =>
      (uri,
        benchmarkQuery(graph, queryFromV(syn(V(getIdFromNode(_: Entity) == id) ^^), query)))
    }
  }
}