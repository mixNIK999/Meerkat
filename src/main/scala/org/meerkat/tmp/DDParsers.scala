package org.meerkat.tmp

import org.meerkat.sppf.NonPackedNode
import org.meerkat.util.Input
import org.meerkat.sppf.SPPFLookup
import org.meerkat.sppf.Slot

object DDParsers { import AbstractCPSParsers._
  
  implicit def obj1[A,B,ValA,ValB](implicit vals: ValA|~|ValB) = new CanBuildSequence[(NonPackedNode,A),(NonPackedNode,B),ValA,ValB] {
    implicit val m1 = obj4[A]; implicit val m2 = obj4[B]
      
    type T = (NonPackedNode,A~B); type V = vals.R
    type Sequence = DDParsers.Sequence[A~B]
    
    def sequence(p: AbstractSequence[T]): Sequence 
      = new Sequence { 
          def apply(input: Input, i: Int, sppfLookup: SPPFLookup) = p(input,i,sppfLookup)
          def size = p.size; def symbol = p.symbol; def ruleType = p.ruleType
          override def reset = p.reset
        }
    def index(a: (NonPackedNode,A)): Int = a._1.rightExtent
    def intermediate(a: (NonPackedNode,A), b: (NonPackedNode,B), p: Slot, sppfLookup: SPPFLookup): T 
      = (sppfLookup.getIntermediateNode(p, a._1, b._1), new ~(a._2, b._2))
      
    type SequenceBuilder = DDParsers.SequenceBuilder[A~B,V]
    def builderSeq(f: Slot => Sequence): SequenceBuilder = new DDParsers.SequenceBuilder[A~B,V] { def apply(slot: Slot) = f(slot) }
  }

  implicit def obj2[A] = new CanBuildAlternative[(NonPackedNode,A)] {
    implicit val m = obj4[A]
    def result(e: (NonPackedNode,A), p: Slot, nt: Head, sppfLookup: SPPFLookup) = (sppfLookup.getNonterminalNode(nt, p, e._1),e._2)
  }
  
  implicit def obj3[A,B,ValA,ValB] = new CanBuildAlternation[(NonPackedNode,A),(NonPackedNode,B),ValA,ValB] {
    implicit val m1 = obj4[A]; implicit val m2 = obj4[B]
    implicit val o1 = obj2[A]; implicit val o2 = obj2[B]
    
    type Alternation = DDParsers.Alternation[B] 
    def alternation(p: AbstractParser[(NonPackedNode,B)]): Alternation
      = new Alternation {
          def apply(input: Input, i: Int, sppfLookup: SPPFLookup) = p(input,i,sppfLookup)
          def symbol = p.symbol.asInstanceOf[org.meerkat.tree.Alt]
          override def reset = p.reset
        }    
    type AlternationBuilder = DDParsers.AlternationBuilder[B,ValB]
    def builderAlt(f: Head => Alternation): AlternationBuilder = new DDParsers.AlternationBuilder[B,ValB] { def apply(head: Head) = f(head) }
  }
  
  implicit def obj4[A] = new Memoizable[(NonPackedNode,A)] {
    type U = (Int,A)
    def value(t: (NonPackedNode, A)): (Int, A) = (t._1.rightExtent, t._2)
  }
  
  implicit def obj5[A,ValA] = new CanBuildNonterminal[(NonPackedNode,A),ValA] {
    implicit val m = obj4[A]
    type Nonterminal = DDParsers.AbstractNonterminal[A,ValA]
    def nonterminal(nt: String, p: AbstractParser[(NonPackedNode,A)]): Nonterminal 
      = new DDParsers.AbstractNonterminal[A,ValA] {
          def apply(input: Input, i: Int, sppfLookup: SPPFLookup) = p(input, i, sppfLookup)
          def symbol = org.meerkat.tree.SimpleNonterminal(nt)
          def name = nt; override def toString = name
          type Value = ValA
          override def reset = p.reset
        }
    type Symbol = DDParsers.AbstractNonterminal[A,ValA]
    def symbol(p: AbstractSymbol[(NonPackedNode,A),ValA]) = new DDParsers.AbstractNonterminal[A,ValA] { 
      def apply(input: Input, i: Int, sppfLookup: SPPFLookup) = p(input,i,sppfLookup)
      def name = p.name; def symbol = p.symbol.asInstanceOf[org.meerkat.tree.Nonterminal]
      override def reset = p.reset
    }
  }
  
  implicit def obj6[B,ValA,ValB](implicit vals: ValA|~|ValB) = new CanBuildSequence[NonPackedNode,(NonPackedNode,B),ValA,ValB] {
    implicit val m1 = Parsers.obj4; implicit val m2 = obj4[B]
    
    type T = (NonPackedNode,B); type V = vals.R  
    type Sequence = DDParsers.Sequence[B]
    
    def sequence(p: AbstractSequence[T]): Sequence 
      = new Sequence { 
          def apply(input: Input, i: Int, sppfLookup: SPPFLookup) = p(input,i,sppfLookup)
          def size = p.size; def symbol = p.symbol; def ruleType = p.ruleType
          override def reset = p.reset
        }    
    def index(a: NonPackedNode): Int = a.rightExtent
    def intermediate(a: NonPackedNode, b: (NonPackedNode,B), p: Slot, sppfLookup: SPPFLookup) = (sppfLookup.getIntermediateNode(p, a, b._1), b._2)
      
    type SequenceBuilder = DDParsers.SequenceBuilder[B,V]
    def builderSeq(f: Slot => Sequence): SequenceBuilder = new DDParsers.SequenceBuilder[B,V] { def apply(slot: Slot) = f(slot) }
  }
  
  implicit def obj7[A,ValA,ValB](implicit vals: ValA|~|ValB) = new CanBuildSequence[(NonPackedNode,A),NonPackedNode,ValA,ValB] {
    implicit val m1 = obj4[A]; implicit val m2 = Parsers.obj4
    
    type T = (NonPackedNode,A); type V = vals.R
    type Sequence = DDParsers.Sequence[A]
    
    def sequence(p: AbstractSequence[T]): Sequence 
      = new Sequence { 
          def apply(input: Input, i: Int, sppfLookup: SPPFLookup) = p(input,i,sppfLookup)
          def size = p.size; def symbol = p.symbol; def ruleType = p.ruleType
          override def reset = p.reset
        }    
    def index(a: (NonPackedNode,A)): Int = a._1.rightExtent
    def intermediate(a: (NonPackedNode,A), b: NonPackedNode, p: Slot, sppfLookup: SPPFLookup) = (sppfLookup.getIntermediateNode(p, a._1, b), a._2)
      
    type SequenceBuilder = DDParsers.SequenceBuilder[A,V]
    def builderSeq(f: Slot => Sequence): SequenceBuilder = new DDParsers.SequenceBuilder[A,V] { def apply(slot: Slot) = f(slot) }
  }
  
  implicit def obj8[A,B,Val] = new CanMap[(NonPackedNode,A),(NonPackedNode,B),Val] {
    implicit val m = obj4[A]
    type Nonterminal = DDParsers.AbstractNonterminal[B,Val]
    def nonterminal(name: String, p: AbstractParser[(NonPackedNode,B)]) = obj5[B,Val].nonterminal(name, p)
    
    def index(a: (NonPackedNode,A)) = a._1.rightExtent
    def intermediate(a: (NonPackedNode,A), b: (NonPackedNode,B), p: Slot, sppfLookup: SPPFLookup) = (sppfLookup.getIntermediateNode(p, a._1, b._1), b._2)
    
    type Sequence = DDParsers.Sequence[B]
    def sequence(p: AbstractSequence[(NonPackedNode,B)]) = new Sequence { 
          def apply(input: Input, i: Int, sppfLookup: SPPFLookup) = p(input,i,sppfLookup)
          def size = p.size; def symbol = p.symbol; def ruleType = p.ruleType
          override def reset = p.reset
        }   
    type SequenceBuilder = DDParsers.SequenceBuilder[B,Val]
    def builderSeq(f: Slot => Sequence) = new DDParsers.SequenceBuilder[B,Val] { type Value = Val; def apply(slot: Slot) = f(slot) }
  }
  
  implicit def obj9[B,Val] = new CanMap[NonPackedNode,(NonPackedNode,B),Val] {
    implicit val m = Parsers.obj4
    type Nonterminal = DDParsers.AbstractNonterminal[B,Val]
    def nonterminal(name: String, p: AbstractParser[(NonPackedNode,B)]) = obj5[B,Val].nonterminal(name, p)
    
    def index(a: NonPackedNode) = a.rightExtent
    def intermediate(a: NonPackedNode, b: (NonPackedNode,B), p: Slot, sppfLookup: SPPFLookup) = (sppfLookup.getIntermediateNode(p, a, b._1), b._2)
    
    type Sequence = DDParsers.Sequence[B]
    def sequence(p: AbstractSequence[(NonPackedNode,B)]) = new Sequence { 
          def apply(input: Input, i: Int, sppfLookup: SPPFLookup) = p(input,i,sppfLookup)
          def size = p.size; def symbol = p.symbol; def ruleType = p.ruleType
          override def reset = p.reset
        }   
    type SequenceBuilder = DDParsers.SequenceBuilder[B,Val]
    def builderSeq(f: Slot => Sequence) = new DDParsers.SequenceBuilder[B,Val] { def apply(slot: Slot) = f(slot) }
  }
  
  trait Sequence[+T] extends AbstractParser[(NonPackedNode,T)] with Slot { def size: Int; def symbol: org.meerkat.tree.Sequence }  
  trait Alternation[+T] extends AbstractParser[(NonPackedNode,T)] { def symbol: org.meerkat.tree.Alt }
  
  trait AbstractNonterminal[+T,+V] extends AbstractParser[(NonPackedNode,T)] { import AbstractParser._
    def name: String
    def symbol: org.meerkat.tree.Nonterminal
    def action: Option[Any => V] = None
        
    def ~ [U,F](p: AbstractNonterminal[U,F])(implicit tuple: V|~|F, layout: Layout) = (this ~~ layout.get).~~(p)(tuple)
    def ~~ [U,F](p: AbstractNonterminal[U,F])(implicit tuple: V|~|F) 
      = seq(this, p)(obj1[T,U,V,F](tuple))

    def ~ [F](p: Parsers.Symbol[F])(implicit tuple: V|~|F, layout: Layout) = (this ~~ layout.get).~~(p)(tuple)  
    def ~~ [F](p: Parsers.Symbol[F])(implicit tuple: V|~|F) = seq(this, p)(obj7[T,V,F](tuple))
    
    def | [U >: T,F >: V](p: AlternationBuilder[U,F]) = altSymAlt(this, p)
    def | [U >: T,F >: V](p: SequenceBuilder[U,F]) = altSymSeq(this, p)
    def | [U >: T,F >: V](p: AbstractNonterminal[U,F]) = altSym(this, p)
    
    def map[U](f: T => U) = AbstractParser.map(this, (t:(NonPackedNode,T)) => (t._1,f(t._2)))(obj8[T,U,V])
    
    def ~>[U,F](f: T => AbstractNonterminal[U,F])(implicit tuple: V|~|F, layout: Layout) 
      = AbstractParser.flatMap(this ~~ layout.get, (t:(NonPackedNode,T)) => f(t._2))(obj1[T,U,V,F](tuple))
    
    def ~>>[F](f: T => Parsers.Symbol[F])(implicit tuple: V|~|F, layout: Layout) 
      = AbstractParser.flatMap(this ~~ layout.get, (t:(NonPackedNode,T)) => f(t._2))(obj7[T,V,F](tuple))
  }
  
  type DataNonterminal[+T] = AbstractNonterminal[T,NoValue]
  
  type DataNonterminalWithAction[+T,+V] = AbstractNonterminal[T,V]
  
  trait SequenceBuilder[+T,+V] extends (Slot => Sequence[T]) { import AbstractParser._
    def action: Option[Any => V] = None
    
    def ~ [U,F](p: AbstractNonterminal[U,F])(implicit tuple: V|~|F, layout: Layout) = (this ~~ layout.get).~~(p)(tuple)
    def ~~ [U,F](p: AbstractNonterminal[U,F])(implicit tuple: V|~|F) = seq(this, p)(obj1[T,U,V,F](tuple))

    def ~ [F](p: Parsers.Symbol[F])(implicit tuple: V|~|F, layout: Layout) = (this ~~ layout.get).~~(p)(tuple)  
    def ~~ [F](p: Parsers.Symbol[F])(implicit tuple: V|~|F) = seq(this, p)(obj7[T,V,F](tuple))

    def | [U >: T,F >: V](p: AlternationBuilder[U,F]) = altSeqAlt(this, p)
    def | [U >: T,F >: V](p: SequenceBuilder[U,F]) = altSeq(this, p)
    def | [U >: T,F >: V](p: AbstractNonterminal[U,F]) = altSeqSym(this, p)
    
    def map[U](f: T => U) = AbstractParser.map(this, (t:(NonPackedNode,T)) => (t._1,f(t._2)))(obj8[T,U,V])
    
    def ~>[U,F](f: T => AbstractNonterminal[U,F])(implicit tuple: V|~|F, layout: Layout) 
      = AbstractParser.flatMap(this ~~ layout.get, (t:(NonPackedNode,T)) => f(t._2))(obj1[T,U,V,F](tuple))
    
    def ~>>[F](f: T => Parsers.Symbol[F])(implicit tuple: V|~|F, layout: Layout) 
      = AbstractParser.flatMap(this ~~ layout.get, (t:(NonPackedNode,T)) => f(t._2))(obj7[T,V,F](tuple))
  }
  
  trait AlternationBuilder[+T,+V] extends (Head => Alternation[T]) { import AbstractParser._
    def action: Option[Any => V] = None
    def | [U >: T,F >: V](p: AlternationBuilder[U,F]) = altAlt(this, p)
    def | [U >: T,F >: V](p: SequenceBuilder[U,F]) = altAltSeq(this, p)
    def | [U >: T,F >: V](p: AbstractNonterminal[U,F]) = altAltSym(this, p)
  }

  implicit class SequenceBuilderOps[V](p: Parsers.SequenceBuilder[V]) { import AbstractParser._
    def ~ [U,F](q: AbstractNonterminal[U,F])(implicit tuple: V|~|F, layout: Layout) = (p ~~ layout.get).~~(q)(tuple)
    def ~~ [U,F](q: AbstractNonterminal[U,F])(implicit tuple: V|~|F) = seq(p, q)(obj6[U,V,F](tuple))
  }
  
  implicit class SymbolOps[V](p: Parsers.Symbol[V]) { import AbstractParser._
    def ~ [U,F](q: AbstractNonterminal[U,F])(implicit tuple: V|~|F, layout: Layout) = (p ~~ layout.get).~~(q)(tuple)
    def ~~ [U,F](q: AbstractNonterminal[U,F])(implicit tuple: V|~|F) = seq(p, q)(obj6[U,V,F](tuple))
  
    def map[U](f: String => U) = AbstractParser.map(p, (input: Input, t: NonPackedNode) => (t,f(input.substring(t.leftExtent, t.rightExtent))))(obj9[U,V])
  }
  
  implicit class StringSeqOps(term: String) { import AbstractParser._
    val p = Parsers.toTerminal(term)
    def ~ [U,F](q: AbstractNonterminal[U,F])(implicit tuple: NoValue|~|F, layout: Layout) = (p ~~ layout.get).~~(q)(tuple) 
    def ~~ [U,F](q: AbstractNonterminal[U,F])(implicit tuple: NoValue|~|F) = seq(p, q)(obj6[U,NoValue,F](tuple))
  }

  def ntAlt[A,V](name: String, p: => AlternationBuilder[A,V]) = nonterminalAlt[(NonPackedNode,A),V](name, p)
  def ntSeq[A,V](name: String, p: => SequenceBuilder[A,V]) = nonterminalSeq[(NonPackedNode,A),V](name, p)
  def ntSym[A,V](name: String, p: => AbstractNonterminal[A,V]) = nonterminalSym[(NonPackedNode,A),V](name, p)
  
}