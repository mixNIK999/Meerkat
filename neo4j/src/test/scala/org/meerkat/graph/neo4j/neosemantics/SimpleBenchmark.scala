package org.meerkat.graph.neo4j.neosemantics

import org.meerkat.Syntax.syn
import org.meerkat.graph.neo4j.Neo4jInput.Entity
import org.meerkat.input.Input
import org.meerkat.parsers.Parsers._
import org.meerkat.parsers._
import org.meerkat.sppf.NonPackedNode
import org.neo4j.graphdb.Node
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
        executeQuery(query, graph).length
      }
      query.reset()
      res
    }
    //    println(list)
    list.map(_.value).sum / runsNumber
  }

  def getIdFromNode(e : Entity): Long = {e.entity.asInstanceOf[Node].getId}

  def benchmarkQuery[L, N, T <: NonPackedNode, V](
                      graph: Input[L, N],
                      query: AbstractCPSParsers.AbstractSymbol[L, N, T, V],
                      runsNumber: Int): (Double, Double, List[V]) = {
    val time = benchmarkQueryWith(graph, query, runsNumber, new Measurer.Default)
    val memory = benchmarkQueryWith(graph, query, runsNumber, new Measurer.MemoryFootprint)
    val res = executeQuery(query, graph).toList
    (time, memory, res)
  }

  // time in ms, memory in kB
  def benchmarkSample[L, N <: Entity, V](
                                          graph: Input[L, N],
                                          queryWithoutStart: Symbol[L, N, V],
                                          runsNumber: Int,
                                          getStartVertex: Symbol[L, N, Entity]) = {

    val allVertex = executeQuery(getStartVertex, graph).map(getIdFromNode)
    for (v <- allVertex) yield {
      val query = syn(V(getIdFromNode(_:Entity) == v) ~ queryWithoutStart &&)
      benchmarkQuery(graph, query, runsNumber)
    }
  }
}
