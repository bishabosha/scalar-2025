package ntquery

// NOTE STRIPPED BACK FOR BASIC USE CASE ONLY

import NamedTuple.{NamedTuple, AnyNamedTuple}

class Table[T]:
  def select: Query[T, false] = ???
  def delete: DeleteQuery[T] = ???
  def insert: InsertQuery[T] = ???

trait InsertQuery[T]
// todo some way to select specific fields?
// partial named tuple?

trait DeleteQuery[T]:
  def filter(f: Expr[T] => Expr[Boolean]): DeleteQuery[T] = ???

trait Query[T, IsScalar <: Boolean]

trait Expr[T] extends Selectable:
  type Fields = NamedTuple.Map[NamedTuple.From[T], Expr]
  def selectDynamic(name: String): Expr[?] = ???

  def ===(that: Expr[T]): Expr[Boolean] = ???

given Conversion[String, Expr[String]] = ???

type RunResult[T, IsScalar] = IsScalar match
  case true  => T
  case false => Seq[T]

trait DB:
  def run[T, IsScalar <: Boolean, Res](q: Query[T, IsScalar])(using
      RunResult[T, IsScalar] =:= Res
  ): Res

def transact(f: DB => Unit): Unit =
  ???
