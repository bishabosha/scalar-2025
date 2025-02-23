package ntdata

import substructural.Sub

import NamedTuple.{NamedTuple, AnyNamedTuple, Names}

type InnerColumns[T] = NamedTuple.From[T] match
  case NamedTuple[ns, vs] =>
    NamedTuple[ns, Tuple.Map[
      Tuple.Zip[ns, vs],
      [X] =>> X match
        case (n, v) => DataFrame[NamedTuple[n *: EmptyTuple, v *: EmptyTuple]]
    ]]

type ContainsAll[X <: Tuple, Y <: Tuple] <: Boolean = X match
  case x *: xs =>
    Tuple.Contains[Y, x] match
      case true  => ContainsAll[xs, Y]
      case false => false
  case EmptyTuple => true

type SubNames[T] = [From <: AnyNamedTuple] =>> ContainsAll[
  Names[From],
  Names[NamedTuple.From[T]]
] =:= true

type FilterNames[N <: Tuple, T] <: AnyNamedTuple = NamedTuple.From[T] match
  case NamedTuple[ns, vs] => FilterNames0[N, ns, vs, EmptyTuple, EmptyTuple]

type FilterNames0[
    Ns <: Tuple,
    Ns1 <: Tuple,
    Vs1 <: Tuple,
    AccN <: Tuple,
    AccV <: Tuple
] <: AnyNamedTuple =
  Ns match
    case n *: ns =>
      FilterName[n, Ns1, Vs1] match
        case Some[v] => FilterNames0[ns, Ns1, Vs1, n *: AccN, v *: AccV]
    case EmptyTuple => NamedTuple[Tuple.Reverse[AccN], Tuple.Reverse[AccV]]

type FilterName[N, Ns1 <: Tuple, Vs1 <: Tuple] <: Option[Any] =
  (Ns1, Vs1) match
    case (N *: ns, v *: vs) => Some[v]
    case (_ *: ns, _ *: vs) => FilterName[N, ns, vs]

class DataFrame[T](data: Array[?]) extends AnyVal with Selectable:
  type Fields = InnerColumns[T]
  def selectDynamic(name: String): DataFrame[?] = ???
  def columns[F <: AnyNamedTuple: SubNames[T]]: DataFrame[FilterNames[Names[F], T]] =
    ???

object DataFrame:
  // sealed trait Expr[T] extends Selectable:
  //   type Fields = NamedTuple.Map[NamedTuple.From[T], Expr]
  // final class Ref[T] extends Expr[T]

  def fromCSV[T](path: String): DataFrame[T] = ???

object demo:
  type Session = (duration: Long, pulse: Long, maxPulse: Long, calories: Double)

  val df: DataFrame[Session] =
    DataFrame.fromCSV[Session]("data.csv")

  val d: DataFrame[(duration: Long)] =
    df.duration

  val pm: DataFrame[(pulse: Long, maxPulse: Long)] =
    df.columns[(pulse: ?, maxPulse: ?)]

  // TODO: expression with aggregations? look at pola.rs and pokemon dataset
