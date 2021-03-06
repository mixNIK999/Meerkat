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

package org.meerkat.parsers

import org.meerkat.sppf.SPPFLookup
import org.meerkat.sppf.Slot
import AbstractCPSParsers.AbstractAlternationBuilder
import AbstractCPSParsers.AbstractNonterminal
import AbstractCPSParsers.AbstractParser
import AbstractCPSParsers.AbstractSequence
import AbstractCPSParsers.AbstractSequenceBuilder
import AbstractCPSParsers.AbstractSymbol
import AbstractCPSParsers.Result
import AbstractCPSParsers.AbstractAlternationBuilder
import AbstractCPSParsers.AbstractNonterminal
import AbstractCPSParsers.AbstractParser
import AbstractCPSParsers.AbstractSequence
import AbstractCPSParsers.AbstractSequenceBuilder
import AbstractCPSParsers.AbstractSymbol
import org.meerkat.input.Input
import org.meerkat.tree.EdgeSymbol

object Rec extends Enumeration {
  type Rec = Value
  val UNDEFINED, LEFT, RIGHT, BOTH = Value
}

object Assoc extends Enumeration {
  type Assoc = Value
  val UNDEFINED, LEFT, RIGHT, ASSOC, NON_ASSOC = Value
}

object AbstractOperatorParsers {
  import AbstractCPSParsers._

  type AbstractOperatorParser[L, N, +T] = (Prec => AbstractParser[L, N, T])

  type AbstractOperatorSequence[L, N, +T, +V] =
    ((Prec, Prec) => AbstractSequenceBuilder[L, N, T, V]) {
      def infix: Boolean; def prefix: Boolean; def postfix: Boolean;
      def assoc: Assoc.Assoc
    }

  type ParserChar = String => (Boolean, String)
  def getParser(ch: Char): ParserChar = (str: String) => {
    if (str != null && str.nonEmpty && str.charAt(0) == ch)
      (true, str.substring(1))
    else (false, "Error")
  }

  type AbstractOperatorAlternation[L, N, +T, +V] =
    Prec => AbstractAlternationBuilder[L, N, T, V]

  type AbstractOperatorSymbol[L, N, +T, +V] = Prec => AbstractSymbol[L, N, T, V]

  type AbstractOperatorNonterminal[L, N, +T, +V] =
    (Prec => AbstractNonterminal[L, N, T, V])

  type Head[L, N] = (Prec => AbstractNonterminal[L, N, Any, Any])

  type AbstractOperatorSequenceBuilder[L, N, +T, +V] =
    Head[L, N] => AbstractOperatorSequence[L, N, T, V]

  type AbstractOperatorAlternationBuilder[L, N, +T, +V] =
    (Head[L, N], Group) => (Group => AbstractOperatorAlternation[L, N, T, V],
                            Group,
                            Option[Group])

  trait CanBuildSequence[L, N, A, B, ValA, ValB] {
    implicit val o: AbstractCPSParsers.CanBuildSequence[L, N, A, B, ValA, ValB]

    type AbstractOperatorSequence = ((Prec, Prec) => o.SequenceBuilder) {
      def infix: Boolean; def prefix: Boolean; def postfix: Boolean;
      def assoc: Assoc.Assoc
    }
    type OperatorSequence <: AbstractOperatorSequence
    def sequence(p: AbstractOperatorSequence): OperatorSequence

    type OperatorSequenceBuilder <: (Head[L, N] => OperatorSequence)
    def builderSeq(f: Head[L, N] => OperatorSequence): OperatorSequenceBuilder
  }

  trait CanBuildAlternation[L, N, A, B >: A, ValA, ValB] {
    implicit val o: AbstractCPSParsers.CanBuildAlternation[L,
                                                           N,
                                                           A,
                                                           B,
                                                           ValA,
                                                           ValB]
    type OperatorAlternation <: Prec => o.AlternationBuilder
    def alternation(f: Prec => o.AlternationBuilder): OperatorAlternation

    type OperatorAlternationBuilder <: (
        Head[L, N],
        Group) => (Group => OperatorAlternation, Group, Option[Group])
    def builderAlt(
        f: (Head[L, N], Group) => (Group => OperatorAlternation,
                                   Group,
                                   Option[Group])): OperatorAlternationBuilder
  }

  trait CanBuildNonterminal[L, N, A, ValA] {
    implicit val o1: AbstractCPSParsers.CanBuildNonterminal[L, N, A, ValA]
    implicit val o2: AbstractCPSParsers.CanBuildAlternative[L, N, A]

    type OperatorNonterminal <: Prec => o1.Nonterminal
    def nonterminal(name: String,
                    f: Prec => o1.Nonterminal): OperatorNonterminal
  }

  object AbstractOperatorParser {

    def seqNt[L, N, A, B, ValA, ValB](
        p1: AbstractOperatorNonterminal[L, N, A, ValA],
        p2: AbstractOperatorNonterminal[L, N, B, ValB])(
        implicit builder: CanBuildSequence[L, N, A, B, ValA, ValB]
    ): builder.OperatorSequenceBuilder = {
      import builder._
      builderSeq { head =>
        val left = p1 == head; val right = p2 == head
        val inx  = if (left && right) true else false
        val prx  = if (left && !right) true else false
        val psx  = if (!left && right) true else false
        sequence(new ((Prec, Prec) => o.SequenceBuilder) {
          type Value = o.V
          def apply(prec1: Prec, prec2: Prec) =
            AbstractParser.seq(p1(prec1), p2(prec2))
          def infix = inx; def prefix = prx; def postfix = psx;
          def assoc = Assoc.UNDEFINED
        })
      }
    }

    def seqOpSeqNt[L, N, A, B, ValA, ValB](
        p1: AbstractOperatorSequenceBuilder[L, N, A, ValA],
        p2: AbstractOperatorNonterminal[L, N, B, ValB]
    )(implicit builder: CanBuildSequence[L, N, A, B, ValA, ValB])
      : builder.OperatorSequenceBuilder = {
      import builder._
      builderSeq { head =>
        val q1   = p1(head)
        val left = q1.infix || q1.postfix; val right = p2 == head
        val inx  = if (left && right) true else false
        val prx  = if (left && !right) true else false
        val psx  = if (!left && right) true else false
        sequence(new ((Prec, Prec) => o.SequenceBuilder) {
          type Value = o.V
          def apply(prec1: Prec, prec2: Prec) =
            AbstractParser.seq(q1(prec1, $), p2(prec2))
          def infix = inx; def prefix = prx; def postfix = psx;
          def assoc = Assoc.UNDEFINED
        })
      }
    }

    def seqOpSeqSym[L, N, A, B, ValA, ValB](
        p1: AbstractOperatorSequenceBuilder[L, N, A, ValA],
        p2: AbstractSymbol[L, N, B, ValB])(
        implicit builder: CanBuildSequence[L, N, A, B, ValA, ValB]
    ): builder.OperatorSequenceBuilder = {
      import builder._
      builderSeq { head =>
        val q1  = p1(head)
        val psx = if (q1.infix || q1.postfix) true else false
        sequence(new ((Prec, Prec) => o.SequenceBuilder) {
          type Value = o.V
          def apply(prec1: Prec, prec2: Prec) =
            AbstractParser.seq(q1(prec1, $), p2)
          def infix = false; def prefix = false; def postfix = psx;
          def assoc = Assoc.UNDEFINED
        })
      }
    }

    def seqNtSym[L, N, A, B, ValA, ValB](
        p1: AbstractOperatorNonterminal[L, N, A, ValA],
        p2: AbstractSymbol[L, N, B, ValB])(
        implicit builder: CanBuildSequence[L, N, A, B, ValA, ValB]
    ): builder.OperatorSequenceBuilder = {
      import builder._
      builderSeq { head =>
        val psx = p1 == head
        sequence(new ((Prec, Prec) => o.SequenceBuilder) {
          type Value = o.V
          def apply(prec1: Prec, prec2: Prec) =
            AbstractParser.seq(p1(prec1), p2)
          def infix = false; def prefix = false; def postfix = psx;
          def assoc = Assoc.UNDEFINED
        })
      }
    }

    def seqSymNt[L, N, A, B, ValA, ValB](
        p1: AbstractSymbol[L, N, A, ValA],
        p2: AbstractOperatorNonterminal[L, N, B, ValB])(
        implicit builder: CanBuildSequence[L, N, A, B, ValA, ValB]
    ): builder.OperatorSequenceBuilder = {
      import builder._
      builderSeq { head =>
        val prx = p2 == head
        sequence(new ((Prec, Prec) => o.SequenceBuilder) {
          type Value = o.V
          def apply(prec1: Prec, prec2: Prec) =
            AbstractParser.seq(p1, p2(prec2))
          def infix = false; def prefix = prx; def postfix = false;
          def assoc = Assoc.UNDEFINED
        })
      }
    }

    def seqSeqNt[L, N, A, B, ValA, ValB](
        p1: AbstractSequenceBuilder[L, N, A, ValA],
        p2: AbstractOperatorNonterminal[L, N, B, ValB])(
        implicit builder: CanBuildSequence[L, N, A, B, ValA, ValB]
    ): builder.OperatorSequenceBuilder = {
      import builder._
      builderSeq { head =>
        val prx = if (p2 == head) true else false
        sequence(new ((Prec, Prec) => o.SequenceBuilder) {
          type Value = o.V
          def apply(prec1: Prec, prec2: Prec) =
            AbstractParser.seq(p1, p2(prec2))
          def infix = false; def prefix = prx; def postfix = false;
          def assoc = Assoc.UNDEFINED
        })
      }
    }

    def altOpAlt[L, N, A, B >: A, ValA, ValB >: ValA](
        p1: AbstractOperatorAlternationBuilder[L, N, A, ValA],
        p2: AbstractOperatorAlternationBuilder[L, N, B, ValB]
    )(implicit builder: CanBuildAlternation[L, N, A, B, ValA, ValB])
      : builder.OperatorAlternationBuilder = {
      import builder._
      builderAlt { (head, group1) =>
        val (f2, opened2, closed2) = p2(head, group1);
        val (f1, opened1, closed1) = p1(head, opened2)
        val closed                 = closed2.orElse(closed1)
        ({ group2 =>
          val q1 = f1(group2); val q2 = f2(closed1.getOrElse(group2))
          alternation { prec =>
            AbstractParser.altAlt(q1(prec), q2(prec))
          }
        }, opened1, closed)
      }
    }

    def altOpAltOpSeq[L, N, A, B >: A, ValA, ValB >: ValA](
        p1: AbstractOperatorAlternationBuilder[L, N, A, ValA],
        p2: AbstractOperatorSequenceBuilder[L, N, B, ValB]
    )(implicit builder: CanBuildAlternation[L, N, A, B, ValA, ValB])
      : builder.OperatorAlternationBuilder = {
      import builder._
      builderAlt { (head, group1) =>
        val s2 = p2(head)
        val (level, group) =
          if (s2.infix || s2.prefix || s2.postfix)
            group1.level(s2.assoc,
                         if (s2.prefix) -1 else if (s2.postfix) 1 else 0)
          else (-1, group1)
        val (f1, opened1, closed1) = p1(head, group)
        ({ group2 =>
          val q1 = f1(group2);
          val q2 = filter(s2, level, closed1.getOrElse(group2))
          alternation { prec =>
            AbstractParser.altAltSeq(q1(prec), q2(prec))
          }
        }, opened1, closed1)
      }
    }

    def altOpAltOpSym[L, N, A, B >: A, ValA, ValB >: ValA](
        p1: AbstractOperatorAlternationBuilder[L, N, A, ValA],
        p2: AbstractOperatorSymbol[L, N, B, ValB]
    )(implicit builder: CanBuildAlternation[L, N, A, B, ValA, ValB])
      : builder.OperatorAlternationBuilder = {
      import builder._
      builderAlt { (head, group1) =>
        val (f1, opened1, closed1) = p1(head, group1)
        ({ group2 =>
          val q1 = f1(group2)
          alternation { prec =>
            AbstractParser.altAltSym(q1(prec), p2(prec))
          }
        }, opened1, closed1)
      }
    }

    def altOpSeqOpAlt[L, N, A, B >: A, ValA, ValB >: ValA](
        p1: AbstractOperatorSequenceBuilder[L, N, A, ValA],
        p2: AbstractOperatorAlternationBuilder[L, N, B, ValB]
    )(implicit builder: CanBuildAlternation[L, N, A, B, ValA, ValB])
      : builder.OperatorAlternationBuilder = {
      import builder._
      builderAlt { (head, group1) =>
        val (f2, opened2, closed2) = p2(head, group1)
        val s1                     = p1(head)
        val (level, group) =
          if (s1.infix || s1.prefix || s1.postfix)
            opened2.level(s1.assoc,
                          if (s1.prefix) -1 else if (s1.postfix) 1 else 0)
          else (-1, opened2)
        ({ group2 =>
          val q1 = filter(s1, level, group2); val q2 = f2(group2)
          alternation { prec =>
            AbstractParser.altSeqAlt(q1(prec), q2(prec))
          }
        }, group, closed2)
      }
    }

    def altOpSeq[L, N, A, B >: A, ValA, ValB >: ValA](
        p1: AbstractOperatorSequenceBuilder[L, N, A, ValA],
        p2: AbstractOperatorSequenceBuilder[L, N, B, ValB]
    )(implicit builder: CanBuildAlternation[L, N, A, B, ValA, ValB])
      : builder.OperatorAlternationBuilder = {
      import builder._
      builderAlt { (head, group1) =>
        val s1 = p1(head); val s2 = p2(head)
        val (l2, g2) =
          if (s2.infix || s2.prefix || s2.postfix)
            group1.level(s2.assoc,
                         if (s2.prefix) -1 else if (s2.postfix) 1 else 0)
          else (-1, group1)
        val (l1, g1) =
          if (s1.infix || s1.prefix || s1.postfix)
            g2.level(s1.assoc, if (s1.prefix) -1 else if (s1.postfix) 1 else 0)
          else (-1, g2)
        ({ group2 =>
          val q1 = filter(s1, l1, group2); val q2 = filter(s2, l2, group2)
          alternation { prec =>
            AbstractParser.altSeq(q1(prec), q2(prec))
          }
        }, g1, None)
      }
    }

    def altOpSeqOpSym[L, N, A, B >: A, ValA, ValB >: ValA](
        p1: AbstractOperatorSequenceBuilder[L, N, A, ValA],
        p2: AbstractOperatorSymbol[L, N, B, ValB]
    )(implicit builder: CanBuildAlternation[L, N, A, B, ValA, ValB])
      : builder.OperatorAlternationBuilder = {
      import builder._
      builderAlt { (head, group1) =>
        val s1 = p1(head)
        val (l1, g1) =
          if (s1.infix || s1.prefix || s1.postfix)
            group1.level(s1.assoc,
                         if (s1.prefix) -1 else if (s1.postfix) 1 else 0)
          else (-1, group1)
        ({ group2 =>
          val q1 = filter(s1, l1, group2)
          alternation { prec =>
            AbstractParser.altSeqSym(q1(prec), p2(prec))
          }
        }, g1, None)
      }
    }

    def altOpSymOpAlt[L, N, A, B >: A, ValA, ValB >: ValA](
        p1: AbstractOperatorSymbol[L, N, A, ValA],
        p2: AbstractOperatorAlternationBuilder[L, N, B, ValB]
    )(implicit builder: CanBuildAlternation[L, N, A, B, ValA, ValB])
      : builder.OperatorAlternationBuilder = {
      import builder._
      builderAlt { (head, group1) =>
        val (f2, opened2, closed2) = p2(head, group1)
        ({ group2 =>
          val q2 = f2(group2)
          alternation { prec =>
            AbstractParser.altSymAlt(p1(prec), q2(prec))
          }
        }, opened2, closed2)
      }
    }

    def altOpSymOpSeq[L, N, A, B >: A, ValA, ValB >: ValA](
        p1: AbstractOperatorSymbol[L, N, A, ValA],
        p2: AbstractOperatorSequenceBuilder[L, N, B, ValB]
    )(implicit builder: CanBuildAlternation[L, N, A, B, ValA, ValB])
      : builder.OperatorAlternationBuilder = {
      import builder._
      builderAlt { (head, group1) =>
        val s2 = p2(head)
        val (l, g) =
          if (s2.infix || s2.prefix || s2.postfix)
            group1.level(s2.assoc,
                         if (s2.prefix) -1 else if (s2.postfix) 1 else 0)
          else (-1, group1)
        ({ group2 =>
          val q2 = filter(s2, l, group2)
          alternation { prec =>
            AbstractParser.altSymSeq(p1(prec), q2(prec))
          }
        }, g, None)
      }
    }

    def altOpSym[L, N, A, B >: A, ValA, ValB >: ValA](
        p1: AbstractOperatorSymbol[L, N, A, ValA],
        p2: AbstractOperatorSymbol[L, N, B, ValB]
    )(implicit builder: CanBuildAlternation[L, N, A, B, ValA, ValB])
      : builder.OperatorAlternation = {
      import builder._
      alternation { prec =>
        AbstractParser.altSym(p1(prec), p2(prec))
      }
    }

    def altSymOpSym[L, N, A, ValA](p: AbstractSymbol[L, N, A, ValA])
      : AbstractOperatorSymbol[L, N, A, ValA] = prec => p

    def altSeqOpSeq[L, N, A, ValA](p: AbstractSequenceBuilder[L, N, A, ValA])
      : AbstractOperatorSequenceBuilder[L, N, A, ValA] =
      head =>
        new ((Prec, Prec) => AbstractSequenceBuilder[L, N, A, ValA]) {
          def apply(prec1: Prec, prec2: Prec) = p
          def infix                           = false
          def prefix                          = false
          def postfix                         = false
          def assoc                           = Assoc.UNDEFINED
      }

    def altAltOpAlt[L, N, A, ValA](p: AbstractAlternationBuilder[L, N, A, ValA])
      : AbstractOperatorAlternationBuilder[L, N, A, ValA] =
      (head, group1) => (group2 => prec => p, group1, None)

    /**
      * If |> is used inside an associativity group, it is ignored, i.e., is equivalent to use of |.
      */
    def greaterOpAlt[L, N, A, B >: A, ValA, ValB >: ValA](
        p1: AbstractOperatorAlternationBuilder[L, N, A, ValA],
        p2: AbstractOperatorAlternationBuilder[L, N, B, ValB]
    )(implicit builder: CanBuildAlternation[L, N, A, B, ValA, ValB])
      : builder.OperatorAlternationBuilder = {
      import builder._
      builderAlt { (head, group1) =>
        val (f2, opened2, closed2) = p2(head, group1)
        if (group1 subgroup) {
          val (f1, opened1, closed1) = p1(head, opened2)
          val closed                 = closed2.orElse(closed1)
          ({ group2 =>
            val q1 = f1(group2); val q2 = f2(closed1.getOrElse(group2))
            alternation { prec =>
              AbstractParser.altAlt(q1(prec), q2(prec))
            }
          }, opened1, closed)
        } else {
          val (f1, opened1, closed1) = p1(head, opened2.group)
          val closed                 = Option(opened2.close)
          ({ group2 =>
            val q1 = f1(group2); val q2 = f2(closed.getOrElse(group2))
            alternation { prec =>
              AbstractParser.altAlt(q1(prec), q2(prec))
            }
          }, opened1, closed)
        }
      }
    }

    def greaterOpSeqOpAlt[L, N, A, B >: A, ValA, ValB >: ValA](
        p1: AbstractOperatorSequenceBuilder[L, N, A, ValA],
        p2: AbstractOperatorAlternationBuilder[L, N, B, ValB]
    )(implicit builder: CanBuildAlternation[L, N, A, B, ValA, ValB])
      : builder.OperatorAlternationBuilder = {
      import builder._
      builderAlt { (head, group1) =>
        val (f2, opened2, closed2) = p2(head, group1)
        val s1                     = p1(head)
        if (group1 subgroup) {
          val (level, group) =
            if (s1.infix || s1.prefix || s1.postfix)
              opened2.level(s1.assoc,
                            if (s1.prefix) -1 else if (s1.postfix) 1 else 0)
            else (-1, opened2)
          ({ group2 =>
            val q1 = filter(s1, level, group2); val q2 = f2(group2)
            alternation { prec =>
              AbstractParser.altSeqAlt(q1(prec), q2(prec))
            }
          }, group, closed2)
        } else {
          val opened1 = opened2.group
          val (level, group) =
            if (s1.infix || s1.prefix || s1.postfix)
              opened1.level(s1.assoc,
                            if (s1.prefix) -1 else if (s1.postfix) 1 else 0)
            else (-1, opened1)
          val closed = Option(opened2.close)
          ({ group2 =>
            val q1 = filter(s1, level, group2);
            val q2 = f2(closed.getOrElse(group2))
            alternation { prec =>
              AbstractParser.altSeqAlt(q1(prec), q2(prec))
            }
          }, opened1, closed)
        }
      }
    }

    def greaterOpAltOpSeq[L, N, A, B >: A, ValA, ValB >: ValA](
        p1: AbstractOperatorAlternationBuilder[L, N, A, ValA],
        p2: AbstractOperatorSequenceBuilder[L, N, B, ValB]
    )(implicit builder: CanBuildAlternation[L, N, A, B, ValA, ValB])
      : builder.OperatorAlternationBuilder = {
      import builder._
      builderAlt { (head, group1) =>
        val s2 = p2(head)
        val (level, group) =
          if (s2.infix || s2.prefix || s2.postfix)
            group1.level(s2.assoc,
                         if (s2.prefix) -1 else if (s2.postfix) 1 else 0)
          else (-1, group1)
        if (group1 subgroup) {
          val (f1, opened1, closed1) = p1(head, group)
          ({ group2 =>
            val q1 = f1(group2);
            val q2 = filter(s2, level, closed1.getOrElse(group2))
            alternation { prec =>
              AbstractParser.altAltSeq(q1(prec), q2(prec))
            }
          }, opened1, closed1)
        } else {
          val (f1, opened1, closed1) = p1(head, group.group)
          val closed                 = Option(group.close)
          ({ group2 =>
            val q1 = f1(group2);
            val q2 = filter(s2, level, closed.getOrElse(group2))
            alternation { prec =>
              AbstractParser.altAltSeq(q1(prec), q2(prec))
            }
          }, opened1, closed)
        }
      }
    }

    def greaterOpSeq[L, N, A, B >: A, ValA, ValB >: ValA](
        p1: AbstractOperatorSequenceBuilder[L, N, A, ValA],
        p2: AbstractOperatorSequenceBuilder[L, N, B, ValB]
    )(implicit builder: CanBuildAlternation[L, N, A, B, ValA, ValB])
      : builder.OperatorAlternationBuilder = {
      import builder._
      builderAlt { (head, group1) =>
        val s2 = p2(head)
        val (l2, g2) =
          if (s2.infix || s2.prefix || s2.postfix)
            group1.level(s2.assoc,
                         if (s2.prefix) -1 else if (s2.postfix) 1 else 0)
          else (-1, group1)
        val s1 = p1(head)
        if (group1 subgroup) {
          val (l1, g1) =
            if (s1.infix || s1.prefix || s1.postfix)
              g2.level(s1.assoc,
                       if (s1.prefix) -1 else if (s1.postfix) 1 else 0)
            else (-1, g2)
          ({ group2 =>
            val q1 = filter(s1, l1, group2); val q2 = filter(s2, l2, group2)
            alternation { prec =>
              AbstractParser.altSeq(q1(prec), q2(prec))
            }
          }, g1, None)
        } else {
          val opened2 = g2.group
          val (l1, g1) =
            if (s1.infix || s1.prefix || s1.postfix)
              opened2.level(s1.assoc,
                            if (s1.prefix) -1 else if (s1.postfix) 1 else 0)
            else (-1, opened2)
          val closed = Option(g2.close)
          ({ group2 =>
            val q1 = filter(s1, l1, group2);
            val q2 = filter(s2, l2, closed.getOrElse(group2))
            alternation { prec =>
              AbstractParser.altSeq(q1(prec), q2(prec))
            }
          }, g1, closed)
        }
      }
    }

    def assocAlt[L, N, A, ValA](
        implicit builder: CanBuildAlternation[L, N, A, A, ValA, ValA]
    ): (builder.OperatorAlternationBuilder,
        Assoc.Assoc) => builder.OperatorAlternationBuilder = {
      import builder._
      { (p, a) =>
        builderAlt { (head, group1) =>
          val (f, opened, closed) = p(head, Subgroup(a, group1));
          val max                 = opened.max
          ({ group2 =>
            alternation(f(opened.close.asInstanceOf[Subgroup].update(group2)))
          }, group1.update(max), closed)
        }
      }
    }

    def nonterminalSym[L, N, A, ValA](
        name: String,
        p: => AbstractOperatorSymbol[L, N, A, ValA])(
        implicit builder: CanBuildNonterminal[L, N, A, ValA]
    ): builder.OperatorNonterminal = {
      import builder._
      lazy val q: OperatorNonterminal =
        nonterminal(
          name,
          prec =>
            AbstractCPSParsers.nonterminalSym(s"$name(${prec._1},${prec._2})",
                                              p(prec)))
      q
    }

    def nonterminalSeq[L, N, A, ValA](
        name: String,
        p: => AbstractOperatorSequenceBuilder[L, N, A, ValA])(
        implicit builder: CanBuildNonterminal[L, N, A, ValA]
    ): builder.OperatorNonterminal = {
      import builder._
      lazy val q: OperatorNonterminal =
        nonterminal(
          name,
          prec =>
            AbstractCPSParsers.nonterminalSeq(s"$name(${prec._1},${prec._2})",
                                              p(q)(prec, prec)))
      q
    }

    def nonterminalAlt[L, N, A, ValA](
        name: String,
        p: => AbstractOperatorAlternationBuilder[L, N, A, ValA])(
        implicit builder: CanBuildNonterminal[L, N, A, ValA]
    ): builder.OperatorNonterminal = {
      import builder._
      lazy val q: OperatorNonterminal = nonterminal(
        name, {
          lazy val parser = {
            val (f, opened, closed) = p(q, Group()); f(opened.close)
          }
          prec =>
            AbstractCPSParsers.nonterminalAlt(s"$name(${prec._1},${prec._2})",
                                              parser(prec))
        }
      )
      q
    }

    def filter[L, N, A, ValA](
        p: AbstractOperatorSequence[L, N, A, ValA],
        l: Int,
        group: Group): Prec => AbstractSequenceBuilder[L, N, A, ValA] = {
      // println(s"Sequence with level: $l, group: $group, assoc: ${p.assoc}")
      if (l == -1) return prec => p(prec, prec)

      // Main condition
      val cond: Prec => Boolean =
        if (p.infix)
          prec => group.parent.max >= prec._1 && group.parent.max >= prec._2
        else if (p.prefix) prec => group.parent.max >= prec._1
        else prec => group.parent.max >= prec._2

      if (!group.climb(l)) {
        // Extra condition when climbing is not possible
        var l1 = 0; var r2 = 0
        val extra: Prec => Boolean =
          if (!group.subgroup) {
            if (l == group.undef) {
              l1 = group.undef; r2 = group.undef;
              prec =>
                true
            } else if (p.infix) {
              p.assoc match {
                case Assoc.LEFT =>
                  l1 = group.undef; r2 = l;
                  prec =>
                    prec._2 != l
                case Assoc.RIGHT =>
                  l1 = l; r2 = group.undef;
                  prec =>
                    prec._1 != l
                case Assoc.NON_ASSOC =>
                  l1 = l; r2 = l;
                  prec =>
                    prec._1 != l && prec._2 != l
              }
            } else if (p.prefix) {
              p.assoc match {
                case Assoc.NON_ASSOC =>
                  r2 = l;
                  prec =>
                    prec._2 != l
              }
            } else {
              p.assoc match {
                case Assoc.NON_ASSOC =>
                  l1 = l;
                  prec =>
                    prec._1 != l
              }
            }
          } else {
            if (p.infix)
              group.assoc match {
                case Assoc.LEFT =>
                  if (l == group.undef) {
                    l1 = group.parent.undef; r2 = group.undef;
                    prec =>
                      !(group.min <= prec._2 && prec._2 <= group.max)
                  } else
                    p.assoc match {
                      case Assoc.RIGHT => {
                        l1 = l; r2 = l;
                        prec =>
                          prec._1 != l && (prec._2 == l || !(group.min <= prec._2 && prec._2 <= group.max))
                      }
                      case Assoc.NON_ASSOC => {
                        l1 = l; r2 = l;
                        prec =>
                          prec._1 != l && !(group.min <= prec._2 && prec._2 <= group.max)
                      }
                      case _ =>
                        throw new RuntimeException(
                          "Ups, this should not have happened!")
                    }
                case Assoc.RIGHT =>
                  if (l == group.undef) {
                    l1 = group.undef; r2 = group.parent.undef;
                    prec =>
                      !(group.min <= prec._1 && prec._1 <= group.max)
                  } else
                    p.assoc match {
                      case Assoc.LEFT => {
                        l1 = l; r2 = l;
                        prec =>
                          prec._2 != l && (prec._1 == l || !(group.min <= prec._1 && prec._1 <= group.max))
                      }
                      case Assoc.NON_ASSOC => {
                        l1 = l; r2 = l;
                        prec =>
                          prec._2 != l && !(group.min <= prec._1 && prec._1 <= group.max)
                      }
                      case _ =>
                        throw new RuntimeException(
                          "Ups, this should not have happened!")
                    }
                case Assoc.NON_ASSOC =>
                  if (l == group.undef) {
                    l1 = group.undef; r2 = group.undef
                    prec =>
                      !(group.min <= prec._1 && prec._1 <= group.max) && !(group.min <= prec._2 && prec._2 <= group.max)
                  } else
                    p.assoc match {
                      case Assoc.LEFT => {
                        l1 = l; r2 = group.undef;
                        prec =>
                          (prec._1 == l || !(group.min <= prec._1 && prec._1 <= group.max)) && !(group.min <= prec._2 && prec._2 <= group.max)
                      }
                      case Assoc.RIGHT => {
                        l1 = group.undef; r2 = l;
                        prec =>
                          !(group.min <= prec._1 && prec._1 <= group.max) && (prec._2 == l || !(group.min <= prec._2 && prec._2 <= group.max))
                      }
                      case _ =>
                        throw new RuntimeException(
                          "Ups, this should not have happened!")
                    }
              } else if (p.prefix)
              group.assoc match {
                case Assoc.LEFT =>
                  if (l == group.undef) {
                    r2 = group.undef;
                    prec =>
                      true
                  } else {
                    r2 = l;
                    prec =>
                      prec._2 != l
                  } // NON_ASSOC case
                case Assoc.RIGHT =>
                  if (l == group.undef) {
                    r2 = group.parent.undef;
                    prec =>
                      !(group.min <= prec._1 && prec._1 <= group.max)
                  } else {
                    r2 = l;
                    prec =>
                      prec._2 != l && !(group.min <= prec._1 && prec._1 <= group.max)
                  } // NON_ASSOC case
                case Assoc.NON_ASSOC =>
                  r2 = l;
                  prec =>
                    !(group.min <= prec._1 && prec._1 <= group.max) && !(group.min <= prec._2 && prec._2 <= group.max)
              } else
              group.assoc match {
                case Assoc.LEFT =>
                  if (l == group.undef) {
                    l1 = group.parent.undef;
                    prec =>
                      !(group.min <= prec._2 && prec._2 <= group.max)
                  } else {
                    l1 = l;
                    prec =>
                      prec._1 != l && !(group.min <= prec._2 && prec._2 <= group.max)
                  } // NON_ASSOC case
                case Assoc.RIGHT =>
                  if (l == group.undef) {
                    l1 = group.undef;
                    prec =>
                      true
                  } else {
                    l1 = l;
                    prec =>
                      prec._1 != l
                  } // NON_ASSOC case
                case Assoc.NON_ASSOC =>
                  l1 = l;
                  prec =>
                    !(group.min <= prec._1 && prec._1 <= group.max) && !(group.min <= prec._2 && prec._2 <= group.max)
              }
          }

        val min               = group.parent.min; val max = group.parent.max;
        val undef             = group.parent.undef
        def choose(prec: Int) = if (min <= prec && prec <= max) undef else prec

        if (group.below.prefix && group.below.postfix)
          prec =>
            if (cond(prec) && extra(prec)) ??? else ??? //AbstractParser.alt(p((l1,choose(prec._2)),(choose(prec._1),r2))) else FAIL
        else if (group.below.prefix)
          prec =>
            if (cond(prec) && extra(prec)) p((l1, undef), (choose(prec._1), r2))
            else FAIL[L, N, A, ValA]
        else if (group.below.postfix)
          prec =>
            if (cond(prec) && extra(prec)) p((l1, choose(prec._2)), (undef, r2))
            else FAIL[L, N, A, ValA]
        else
          prec =>
            if (cond(prec) && extra(prec)) p((l1, undef), (undef, r2))
            else FAIL[L, N, A, ValA]

      } else {
        if (!group.subgroup || (group.subgroup && group.min == group.max)) {
          (if (!group.subgroup) p.assoc else group.assoc) match {
            case Assoc.UNDEFINED =>
              if (group.below.prefix && group.below.postfix)
                prec =>
                  if (cond(prec)) p((l, prec._2), (prec._1, l))
                  else FAIL[L, N, A, ValA]
              else if (group.below.prefix)
                prec =>
                  if (cond(prec)) p((l, l), (prec._1, l))
                  else FAIL[L, N, A, ValA]
              else if (group.below.postfix)
                prec =>
                  if (cond(prec)) p((l, prec._2), (l, l))
                  else FAIL[L, N, A, ValA]
              else
                prec =>
                  if (cond(prec)) p((l, l), (l, l)) else FAIL[L, N, A, ValA]
            case Assoc.LEFT =>
              if (group.below.prefix && group.below.postfix)
                prec =>
                  if (cond(prec)) p((l, prec._2), (prec._1, l + 1))
                  else FAIL[L, N, A, ValA]
              else if (group.below.prefix)
                prec =>
                  if (cond(prec)) p((l, l), (prec._1, l + 1))
                  else FAIL[L, N, A, ValA]
              else if (group.below.postfix)
                prec =>
                  if (cond(prec)) p((l, prec._2), (l + 1, l + 1))
                  else FAIL[L, N, A, ValA]
              else
                prec =>
                  if (cond(prec)) p((l, l), (l + 1, l + 1))
                  else FAIL[L, N, A, ValA]
            case Assoc.RIGHT =>
              if (group.below.prefix && group.below.postfix)
                prec =>
                  if (cond(prec)) p((l + 1, prec._2), (prec._1, l))
                  else FAIL[L, N, A, ValA]
              else if (group.below.prefix)
                prec =>
                  if (cond(prec)) p((l + 1, l + 1), (prec._1, l))
                  else FAIL[L, N, A, ValA]
              else if (group.below.postfix)
                prec =>
                  if (cond(prec)) p((l + 1, prec._2), (l, l))
                  else FAIL[L, N, A, ValA]
              else
                prec =>
                  if (cond(prec)) p((l + 1, l + 1), (l, l))
                  else FAIL[L, N, A, ValA]
            case Assoc.NON_ASSOC =>
              // TODO: extra condition for unary operators (non-assoc group that has unary cannot climb !!!)
              if (group.below.prefix && group.below.postfix)
                prec =>
                  if (cond(prec)) p((l + 1, prec._2), (prec._1, l + 1))
                  else FAIL[L, N, A, ValA]
              else if (group.below.prefix)
                prec =>
                  if (cond(prec)) p((l + 1, l + 1), (prec._1, l + 1))
                  else FAIL[L, N, A, ValA]
              else if (group.below.postfix)
                prec =>
                  if (cond(prec)) p((l + 1, prec._2), (l + 1, l + 1))
                  else FAIL[L, N, A, ValA]
              else
                prec =>
                  if (cond(prec)) p((l + 1, l + 1), (l + 1, l + 1))
                  else FAIL[L, N, A, ValA]
          }
        } else {
          // TODO: for each level that is not equal to undef, add extra unequality constraints
          ???
        }
      }
    }

    def FAIL[L, N, A, ValA]: AbstractSequenceBuilder[L, N, A, ValA] =
      new ((Slot) => AbstractSequence[L, N, A]) {
        def apply(slot: Slot) = new AbstractParser[L, N, A] with Slot {
          def apply(input: Input[L, N],
                    i: Int,
                    sppfLookup: SPPFLookup[L, N]): Result[A] = CPSResult.failure
          def size                                           = 0
          def symbol                                         = org.meerkat.tree.Sequence(EdgeSymbol("_FAILURE_"))
          def ruleType =
            org.meerkat.tree
              .PartialRule(slot.ruleType.head, slot.ruleType.body, size)
        }
        def action: Option[Any => ValA] = None
      }

  }

  class Unary(val prefix: Boolean, val postfix: Boolean) {
    override def toString = s"unary($prefix, $postfix)"
  }

  object Unary { def apply() = new Unary(false, false) }

  class Group(val assoc: Assoc.Assoc,
              val min: Int,
              val max: Int,
              val undef: Int,
              val here: Unary,
              val below: Unary) {

    def subgroup = false
    def parent   = this

    def group =
      if (min == max || max - min == 1)
        new Group(
          assoc,
          max,
          max,
          -1,
          Unary(),
          new Unary(here.prefix || below.prefix, here.postfix || below.postfix)
        )
      else
        new Group(
          assoc,
          max + 1,
          max + 1,
          -1,
          Unary(),
          new Unary(here.prefix || below.prefix, here.postfix || below.postfix)
        )

    def update(max: Int) = new Group(assoc, min, max, undef, here, below)
    def update(max: Int, undef: Int, here: Unary) =
      new Group(assoc, min, max, undef, here, below)

    def close =
      if (min == max) this
      else if (max - min == 1)
        new Group(assoc, min, max - 1, undef, here, below)
      else {
        // A group with different levels
        if (undef != -1) new Group(assoc, min, max - 1, undef, here, below)
        else new Group(assoc, min, max, max, here, below)
      }

    /**
      *  @param unary (-1 to indicate prefix, 1 to indicate postfix)
      *  Note: LEFT and RIGHT associativity have no effect on prefix and postfix operators
      */
    def level(assoc: Assoc.Assoc, unary: Int = 0): (Int, Group) = {
      var undef = this.undef; var max = this.max
      val level =
        if (assoc == this.assoc || assoc == Assoc.UNDEFINED
            || ((unary == -1 || unary == 1) && assoc != Assoc.NON_ASSOC)) {
          if (undef != -1) undef else { undef = max; max += 1; undef }
        } else { max += 1; this.max }
      val here = new Unary(if (unary == -1) true else this.here.prefix,
                           if (unary == 1) true else this.here.postfix)
      (level, this.update(max, undef, here))
    }

    def climb(level: Int): Boolean = min == max

    override def toString = s"Group($assoc, $min, $max, $undef, $here, $below)"

  }

  object Group {
    def apply() = new Group(Assoc.UNDEFINED, 1, 1, -1, Unary(), Unary())
  }

  class Subgroup(override val assoc: Assoc.Assoc,
                 override val min: Int,
                 override val max: Int,
                 override val undef: Int,
                 override val here: Unary,
                 override val below: Unary,
                 override val parent: Group)
      extends Group(assoc, min, max, undef, here, below) {

    override def subgroup = true

    override def group =
      new Subgroup(
        assoc,
        max,
        max,
        undef,
        Unary(),
        new Unary(here.prefix || below.prefix, here.postfix || below.postfix),
        parent
      )

    override def update(max: Int) =
      new Subgroup(assoc, min, max, undef, here, below, parent)
    override def update(max: Int, undef: Int, here: Unary) =
      new Subgroup(assoc, min, max, undef, here, below, parent)

    override def close =
      if (min == max) this
      else new Subgroup(assoc, min, max - 1, undef, here, below, parent)

    def update(parent: Group) =
      new Subgroup(assoc, min, max, undef, here, below, parent)

    override def climb(level: Int): Boolean =
      this.min == parent.min && this.max == parent.max && level == this.undef

    override def toString =
      s"Subgroup($assoc,$min,$max,$undef,$here,$below,$parent)"

  }

  object Subgroup {
    def apply(assoc: Assoc.Assoc, parent: Group) =
      new Subgroup(
        assoc,
        parent.max,
        parent.max,
        -1,
        parent.here,
        parent.below,
        parent
      )
  }

}
