package org.meerkat.parsers.graph.neo4j

import org.meerkat.Syntax._
import org.meerkat.parsers.Parsers._
import org.meerkat.parsers._

import org.neo4j.graphdb.GraphDatabaseService

class Neo4jGraphTest3 extends Neo4jGraphTest("3") {
  val S: Nonterminal = syn(
    "a" ~~ "b" ~~ S
      | epsilon
  )

  override def fillDb(db: GraphDatabaseService): Unit = {
    val n1 = db.createNode
    val n2 = db.createNode
    n1.createRelationshipTo(n2, () => "a")
    n2.createRelationshipTo(n1, () => "b")
  }

  override def createParser: Nonterminal = S

  override def expectedSppfStatistics: SPPFStatistics =
    SPPFStatistics(1, 2, 3, 4, 1)
}
