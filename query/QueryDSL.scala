package ntquery

// NOTE STRIPPED BACK FOR BASIC USE CASE ONLY

import NamedTuple.{NamedTuple, AnyNamedTuple, Names, DropNames}
import Tuple.IsMappedBy
import Utils.*
import ntquery.DeleteQuery.Filtered
import scala.annotation.targetName

trait Table[T: TableSchema] extends Product:
  final def schema: TableSchema[T] = summon[TableSchema[T]]
  final def name: String = productPrefix
  final def select: SelectQuery[T, false] = SelectQuery.SelectFrom(this)
  final def delete: DeleteQuery[T] = DeleteQuery.DeleteFrom(this)
  final def insert: InsertQuery[T] = InsertQuery.InsertTo(this)

sealed trait TableSchema[T]:
  def names: IArray[String]
  def fields: IArray[ColSchema[?]]
  def field(name: String): Option[ColSchema[?]]
  def construct(p: Product): T

object TableSchema:
  final class NTTableSchema[T](ns: Tuple, fs: Tuple, f: Product => T) extends TableSchema[T]:
    val names: IArray[String] = ns.toIArray.map(_.asInstanceOf[String])
    val fields: IArray[ColSchema[?]] = fs.toIArray.map(_.asInstanceOf[ColSchema[?]])
    private val lookup: Map[String, ColSchema[?]] = names.iterator.zip(fields.iterator).toMap
    def field(name: String): Option[ColSchema[?]] = lookup.get(name)
    def construct(p: Product): T = f(p)

  transparent inline given [T]: TableSchema[T] =
    NTTableSchema[T](
      compiletime.constValueTuple[Names[NamedTuple.From[T]]],
      compiletime.summonAll[Tuple.Map[DropNames[NamedTuple.From[T]], ColSchema]],
      compiletime.summonFrom {
        case m: scala.deriving.Mirror.ProductOf[T] => m.fromProduct(_)
        case ev: (T <:< AnyNamedTuple)             => Tuple.fromProduct(_).asInstanceOf[T]
      }
    )

enum ColSchema[T]:
  case Str extends ColSchema[String]

object ColSchema:
  given StrIsGiven: ColSchema[String] = ColSchema.Str

sealed trait InsertQuery[T]:
  import InsertQuery.*

  private final def findTable: Table[T] = this match
    case InsertTo(t)         => t
    case Values(inner, _, _) => inner.findTable

  lazy val table: Table[T] = findTable

  // TODO: add constraints
  def values[N <: Tuple: {HasNames as names}, V <: Tuple: ExprLiftable.Id](
      v: NamedTuple[N, V]
  ): InsertQuery[T] = InsertQuery.Values(this, names, v.toTuple.lift)

object InsertQuery:
  case class InsertTo[T](schema: Table[T]) extends InsertQuery[T]
  case class Values[T, N <: Tuple, V <: Tuple](
      base: InsertQuery[T],
      names: HasNames[N],
      values: Expr[V]
  ) extends InsertQuery[T]

sealed trait DeleteQuery[T]:
  import DeleteQuery.*

  private final def findTable: Table[T] = this match
    case DeleteFrom(t)      => t
    case Filtered(inner, _) => inner.findTable

  lazy val table: Table[T] = findTable

  def filter(f: Expr[T] => Expr[Boolean]): DeleteQuery[T] =
    Filtered(this, f(Expr.Ref(Expr.RefId(table))))

object DeleteQuery:
  case class DeleteFrom[T](schema: Table[T]) extends DeleteQuery[T]
  case class Filtered[T](inner: DeleteQuery[T], filtered: Expr[Boolean]) extends DeleteQuery[T]

sealed trait SelectQuery[T, IsScalar <: Boolean]
object SelectQuery:
  case class SelectFrom[T](table: Table[T]) extends SelectQuery[T, false]

object Utils:
  case class HasNames[N <: Tuple](names: Tuple):
    def iterator: Iterator[String] = names.productIterator.map(_.asInstanceOf[String])

  object HasNames:
    transparent inline given [N <: Tuple]: HasNames[N] =
      HasNames[N](
        compiletime.constValueTuple[N]
      )

object ExprLiftable:
  type Id[T] = ExprLiftable[T] { type Res = Expr[T] }
  type As[T] = [U] =>> ExprLiftable[U] { type Res = Expr[T] }

  given ExprIsLiftable: [T] => ExprLiftable[Expr[T]]:
    type Res = Expr[T]
    extension (value: Expr[T]) def lift: Expr[T] = value

  given ExprLiftable[String]:
    type Res = Expr[String]
    extension (value: String) def lift: Expr[String] = Expr.StrLit(value)

  given ExprLiftable[EmptyTuple] with
    type Res = Expr[EmptyTuple]
    extension (value: EmptyTuple) def lift: Expr[EmptyTuple] = Expr.Tup[EmptyTuple](Vector.empty)

  given [H: ExprLiftable.Id, T <: Tuple: ExprLiftable.Id]: ExprLiftable[H *: T] with
    type Res = Expr[H *: T]
    extension (value: H *: T)
      def lift: Expr[H *: T] =
        value.head.lift *: value.tail.lift

trait ExprLiftable[T]:
  type Res <: Expr[?]
  extension (value: T) def lift: Res

sealed trait Expr[T] extends Selectable:
  type Fields = NamedTuple.Map[NamedTuple.From[T], Expr]
  def selectDynamic(name: String): Expr[?] = Expr.Sel(this, name)

  def ===[U: ExprLiftable.As[T]](that: U): Expr[Boolean] = Expr.Eq(this, that.lift)

object Expr:
  final class RefId[T](val table: Table[T]):
    override def toString(): String =
      s"${table.name}.id(0x${Integer.toHexString(hashCode()).take(5)})"
  case class Ref[T](id: RefId[T]) extends Expr[T]
  case class Sel[X, T](expr: Expr[X], name: String) extends Expr[T]
  case class StrLit(value: String) extends Expr[String]
  case class Tup[T <: Tuple](values: Vector[Expr[?]]) extends Expr[T]
  case class Eq[T](left: Expr[T], right: Expr[T]) extends Expr[Boolean]

  extension [H](e: Expr[H])
    def *:[T <: Tuple](t: Expr[T]): Expr[H *: T] = t match
      case Tup(values)      => Tup(values.prepended(e))
      case Ref(_)           => sys.error("tried to concat to a ref!")
      case Sel(_, tupField) => sys.error(s"tried to concat to a selection .$tupField!")

type RunResult[T, IsScalar] = IsScalar match
  case true  => T
  case false => Seq[T]

trait DB:

  def run[T](q: InsertQuery[T]): T
  def run[T](q: DeleteQuery[T]): Unit

  @targetName("runSingle")
  def run[T](q: SelectQuery[T, false]): Seq[T]
  def run[T](q: SelectQuery[T, true]): T
