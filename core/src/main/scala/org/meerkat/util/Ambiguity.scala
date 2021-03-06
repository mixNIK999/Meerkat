/*
 * Copyright (c) 2015, Anastasia Izmaylova and Ali Afroozeh, Centrum Wiskunde & Informatica (CWI)
 * All rights reserved.
 *
 * Redistribution and use in source and binary forms, with or without
 * modification, are permitted provided that the following conditions are met:
 *
 * 1. Redistributions of source code must retain the above copyright notice, this
 *    list of conditions and the following disclaimer.
 *
 * 2. Redistributions in binary form must reproduce the above copyright notice, this
 *    list of conditions and the following disclaimer in the documentation and/or
 *    other materials provided with the distribution.
 *
 * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS" AND
 * ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED
 * WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE DISCLAIMED.
 * IN NO EVENT SHALL THE COPYRIGHT HOLDER OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT,
 * INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT
 * NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA,
 * OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY,
 * WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE)
 * ARISING IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY
 * OF SUCH DAMAGE.
 *
 */

package org.meerkat.util

import org.meerkat.sppf._
import scala.collection.mutable

object Ambiguity {

  def countAmbiguities(node: SPPFNode): Int = {
    val ambiguousNodes: mutable.Set[SPPFNode] = mutable.HashSet.empty
    countAmbiguities(node, ambiguousNodes, mutable.HashSet.empty)
//    for(n <- ambiguousNodes) {
//      Visualization.toDot(n)
//      return ambiguousNodes.size
//    }
    ambiguousNodes.size
  }

  def countAmbiguities(node: SPPFNode,
                       ambiguousNodes: mutable.Set[SPPFNode],
                       duplicateSet: mutable.Set[SPPFNode]): Unit =
    if (!duplicateSet.contains(node)) {
      duplicateSet.add(node)

      node match {
        case n: NonterminalNode =>
          if (n.isAmbiguous) ambiguousNodes.add(n)
          for (t <- n.children)
            countAmbiguities(t, ambiguousNodes, duplicateSet)

        case n: IntermediateNode =>
          if (n.isAmbiguous) ambiguousNodes.add(n)
          for (t <- n.children)
            countAmbiguities(t, ambiguousNodes, duplicateSet)

        case n: EdgeNode[_] =>
        case n: PackedNode =>
          if (n.leftChild != null)
            countAmbiguities(n.leftChild, ambiguousNodes, duplicateSet)
          countAmbiguities(n.rightChild, ambiguousNodes, duplicateSet)
      }
    }

}
