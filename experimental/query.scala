package query.scalasql

import language.experimental.namedTuples
import NamedTuple.{NamedTuple, AnyNamedTuple}

object upstream:
  // Define your table model classes
  case class City[T[_]](
      id: T[Int],
      name: T[String],
      countryCode: T[String],
      district: T[String],
      population: T[Long]
  )
  object City extends Table[City]

  dbClient.transaction: db =>
    val allCities: Seq[City[Sc]] = db.run(City.select)

    // Adding up population of all cities in China
    val citiesPop: Long = db.run(
      City.select.filter(_.countryCode === "CHN").map(_.population).sum
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

  type Sc[T] = T

  class Table[F[_[_]]]:
    def select: Query[F[Expr], false] = ???

  given Conversion[String, Expr[String]] = ???

  class Expr[T]:
    def ===(that: Expr[T]): Expr[Boolean] = ???

  trait QueryMapper[Z]:
    type Res

  given [T]: QueryMapper[Expr[T]] with
    type Res = T

  given [T, U]: QueryMapper[(Expr[T], Expr[U])] with
    type Res = (T, U)

  class Query[T, IsScalar <: Boolean]:
    def filter(f: T => Expr[Boolean]): Query[T, false] = ???
    def map[Z](f: T => Z)(using m: QueryMapper[Z]): Query[m.Res, IsScalar] = ???
    def sum(implicit num: Numeric[T]): Query[T, true] = ???
    def sortBy[U](f: T => Expr[U])(using IsScalar =:= false): Query[T, false] = ???
    def desc(using IsScalar =:= false): Query[T, false] = ???
    def drop(n: Int)(using IsScalar =:= false): Query[T, false] = ???
    def take(n: Int)(using IsScalar =:= false): Query[T, false] = ???

  type ExQ[T] = T match
    case Expr[t] => t

  type RunResult[T, IsScalar] = IsScalar match
    case true  => T
    case false => Seq[T]

  trait DB:
    @scala.annotation.targetName("runF")
    def run[G[_[_]], IsScalar <: Boolean, Res](q: Query[G[Expr], IsScalar])(using
        RunResult[G[Sc], IsScalar] =:= Res
    ): Res =
      ???
    def run[T, IsScalar <: Boolean, Res](q: Query[T, IsScalar])(using RunResult[T, IsScalar] =:= Res): Res =
      ???

  object dbClient:
    def transaction(f: DB => Unit): Unit =
      ???

end upstream

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

class Table[T]:
  def select: Query[T, false] = ???

trait Query[T, IsScalar <: Boolean]:
  def filter(f: Expr[T] => Expr[Boolean]): Query[T, false] = ???
  def map[U](f: Expr[T] => Expr[U]): Query[U, IsScalar] = ???
  @scala.annotation.targetName("mapTuple")
  def map[U <: Tuple: Tuple.IsMappedBy[Expr]](f: Expr[T] => U): Query[Tuple.InverseMap[U, Expr], IsScalar] = ???
  def sortBy[U](f: Expr[T] => Expr[U])(using IsScalar =:= false): Query[T, false] = ???
  def desc(using IsScalar =:= false): Query[T, false] = ???
  def sum(using num: Numeric[T])(using IsScalar =:= false): Query[T, true] = ???
  def drop(n: Int)(using IsScalar =:= false): Query[T, false] = ???
  def take(n: Int)(using IsScalar =:= false): Query[T, false] = ???

trait Expr[T] extends Selectable:
  type Fields = NamedTuple.Map[NamedTuple.From[T], Expr]
  def selectDynamic(name: String): Expr[?] = ???

  def ===(that: Expr[T]): Expr[Boolean] = ???

given Conversion[String, Expr[String]] = ???

type RunResult[T, IsScalar] = IsScalar match
  case true  => T
  case false => Seq[T]

trait DB:
  def run[T, IsScalar <: Boolean, Res](q: Query[T, IsScalar])(using RunResult[T, IsScalar] =:= Res): Res =
    ???

def transact(f: DB => Unit): Unit =
  ???
