package query.scalasql

import language.experimental.namedTuples
import NamedTuple.{NamedTuple, AnyNamedTuple}
// based upon com-lihaoyi/scalasql

case class City(
    id: Int,
    name: String,
    countryCode: String,
    district: String,
    population: Long
)
object City extends Table[City]

@main def demo =
  transact: db =>
    val allCities: Seq[City] = db.run(City.select)

    // Adding up population of all cities in Poland
    val citiesPop: Long = db.run(
      City.select.filter(_.countryCode === "POL").map(_.population).sum
    )
    // Finding the 5-8th largest cities by population
    val fewLargestCities: Seq[(String, Long)] = db.run(
      City.select
        .sortBy(_.population)
        .desc
        .drop(5)
        .take(3)
        .map(c => (c.name, c.population))
    )

enum Shape:
  case Scalar, Vec

object Shape:
  type Scalar = Shape.Scalar.type
  type Vec = Shape.Vec.type

class Table[T]:
  def select: Query[T, Shape.Vec] = ???

trait Query[T, S <: Shape]:
  def filter(f: Expr[T] => Expr[Boolean]): Query[T, Shape.Vec] = ???
  def map[U](f: Expr[T] => Expr[U]): Query[U, S] = ???
  @scala.annotation.targetName("mapTuple")
  def map[U <: Tuple: Tuple.IsMappedBy[Expr]](
      f: Expr[T] => U
  ): Query[Tuple.InverseMap[U, Expr], S] = ???
  def sortBy[U](f: Expr[T] => Expr[U])(using S =:= Shape.Vec): Query[T, Shape.Vec] = ???
  def desc(using S =:= Shape.Vec): Query[T, Shape.Vec] = ???
  def sum(using num: Numeric[T])(using S =:= Shape.Vec): Query[T, Shape.Scalar] = ???
  def drop(n: Int)(using S =:= Shape.Vec): Query[T, Shape.Vec] = ???
  def take(n: Int)(using S =:= Shape.Vec): Query[T, Shape.Vec] = ???

trait Expr[T] extends Selectable:
  type Fields = NamedTuple.Map[NamedTuple.From[T], Expr]
  def selectDynamic(name: String): Expr[?] = ???

  def ===(that: Expr[T]): Expr[Boolean] = ???

given Conversion[String, Expr[String]] = ???

type RunResult[T, S <: Shape] = S match
  case Shape.Scalar => T
  case Shape.Vec    => Seq[T]

trait DB:
  def run[T, S <: Shape, Res](q: Query[T, S])(using
      RunResult[T, S] =:= Res
  ): Res =
    ???

def transact(f: DB => Unit): Unit =
  ???
