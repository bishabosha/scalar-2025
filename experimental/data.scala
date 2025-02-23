package ntdata

import substructural.Sub

import scala.util.chaining.*

import NamedTuple.{NamedTuple, AnyNamedTuple, Names, DropNames}
import java.time.LocalDate
import scala.util.TupledFunction

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
  def merge[U](other: DataFrame[U])(using NamedTuple.From[T] =:= NamedTuple.From[U]): DataFrame[T] =
    ???
  def withValue[F <: AnyNamedTuple](f: F)(using
      Tuple.Disjoint[Names[F], Names[NamedTuple.From[T]]] =:= true
  ): DataFrame[NamedTuple.Concat[NamedTuple.From[T], F]] =
    ???

  def withComputed[F <: AnyNamedTuple](f: DataFrame.Ref[T] => F)(using
      Tuple.Disjoint[Names[F], Names[NamedTuple.From[T]]] =:= true,
      Tuple.IsMappedBy[DataFrame.Expr][DropNames[F]]
  ): DataFrame[NamedTuple.Concat[NamedTuple.From[T], DataFrame.StripExpr[F]]] =
    ???

  def collectOn[F <: AnyNamedTuple: SubNames[T]](using
      NamedTuple.Size[F] =:= 1
  ): DataFrame.Collected[FilterNames[Names[F], T], T] =
    ???

object DataFrame:
  trait Collected[Col <: AnyNamedTuple, T]:
    def keys: DataFrame[Col]
    def get(filter: Tuple.Head[DropNames[Col]]): Option[DataFrame[T]]
    def columns[F <: AnyNamedTuple: SubNames[T]]: Collected[Col, FilterNames[Names[F], T]] =
      ???

  sealed trait Expr[T] extends Selectable:
    type Fields = NamedTuple.Map[NamedTuple.From[T], Expr]
    def selectDynamic(name: String): Expr[?] = Selection(this, name)

  type Func[G] = G match
    case (g => r) => Tuple.Map[g, Expr] => Expr[r]

  type In[G] <: Tuple = G match
    case (g => _) => g & Tuple

  type Out[G] = G match
    case (_ => r) => r

  type StripExpr[T <: AnyNamedTuple] = NamedTuple.Map[
    T,
    [X] =>> X match
      case Expr[t] => t
  ]

  def splice[F, G](f: F)(using TupledFunction[F, G])(in: Tuple.Map[In[G], Expr]): Expr[Out[G]] = ???

  final class Ref[T] extends Expr[T]
  final class Selection[T](inner: Expr[T], name: String) extends Expr[T]

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

object bankaccs:
  type Hsbc =
    (id: String, date: LocalDate, kind: String, merchant: String, diff: BigDecimal)
  type Monzo =
    (id: String, date: LocalDate, category: String, kind: String, name: String, diff: BigDecimal)
  type CreditSuisse =
    (id: String, date: LocalDate, category: String, merchant: String, diff: BigDecimal)

  val hsbc: DataFrame[Hsbc] = ???
  val monzo: DataFrame[Monzo] = ???
  val creditsuisse: DataFrame[CreditSuisse] = ???

  type Cols = (acc_kind: ?, id: ?, cat_computed: ?)

  def hsbcCat(kind: String, merchant: String): String = ???
  def monzoCat(category: String, kind: String, name: String): String = ???
  def creditsuisseCat(category: String, merchant: String): String = ???

  val all =
    hsbc
      .withValue((acc_kind = "hsbc"))
      .withComputed: row =>
        (cat_computed = DataFrame.splice(hsbcCat)((row.kind, row.merchant)))
      .columns[Cols]
      .merge:
        monzo
          .withValue((acc_kind = "monzo"))
          .withComputed: row =>
            (cat_computed = DataFrame.splice(monzoCat)((row.category, row.kind, row.name)))
          .columns[Cols]
      .merge:
        creditsuisse
          .withValue((acc_kind = "creditsuisse"))
          .withComputed: row =>
            (cat_computed = DataFrame.splice(creditsuisseCat)((row.category, row.merchant)))
          .columns[Cols]

  val byKind = all.collectOn[(acc_kind: ?)].columns[(id: ?, cat_computed: ?)]
  val kinds = byKind.keys
  val hsbcAgg = byKind.get("hsbc").get
  val monzoAgg = byKind.get("monzo").get
  val creditsuisseAgg = byKind.get("creditsuisse").get
