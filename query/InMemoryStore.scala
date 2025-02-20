package ntquery

import scala.reflect.ClassTag
import scala.collection.BuildFrom
import scala.collection.mutable
import scala.annotation.targetName

trait Interp:
  def insert[T](q: InsertQuery[T]): Seq[T]
  def select[T, IsScalar <: Boolean](q: SelectQuery[T, IsScalar]): Seq[T]
  def delete[T](q: DeleteQuery[T]): Unit

object InMemoryStore:
  given [Z, C: ClassTag] => BuildFrom[IArray[Z], C, IArray[C]] =
    new BuildFrom[IArray[Z], C, IArray[C]] {
      override def newBuilder(from: IArray[Z]): mutable.Builder[C, IArray[C]] =
        IArray.newBuilder[C]
      override def fromSpecific(from: IArray[Z])(it: IterableOnce[C]): IArray[C] =
        IArray.from(it)
    }

final class InMemoryStore extends DB with Interp:
  import InMemoryStore.given

  override def run[T](q: InsertQuery[T]): T = insert(q).head

  override def run[T](q: DeleteQuery[T]): Unit = delete(q)

  @targetName("runSingle")
  override def run[T](q: SelectQuery[T, false]): Seq[T] = select(q)

  override def run[T](q: SelectQuery[T, true]): T = select(q).head

  private var idCounter = 0
  // TODO: requires global id counter for all tables
  private var data: Map[String, Data[?]] = Map.empty

  case class Data[T](table: Table[T], rec: Map[String, Any])

  def allRecordCount = data.size
  def getData[T](table: Table[T], id: String): Option[Map[String, Any]] =
    data.get(id).collect { case Data(`table`, rec) =>
      rec
    }

  def decode[T](data: Data[T]): T =
    val schema = data.table.schema
    schema.construct(
      // TODO: optimisation - hint if tuple based, i.e. identity,
      // then otherwise use array wrapper?
      Tuple.fromIArray(
        schema.names
          .lazyZip(schema.fields)
          .map((n, s) =>
            s match
              case ColSchema.Str => data.rec(n).asInstanceOf[String]
          )
      )
    )

  def insert[T](q: InsertQuery[T]): Seq[T] = q match
    case InsertQuery.InsertTo(schema) => Seq.empty
    case InsertQuery.Values(base, names, values) =>
      val table = base.table
      val valuesT = values match
        case Expr.Tup(values) =>
          values.map:
            case Expr.StrLit(value)   => value
            case Expr.Ref(_)          => sys.error("no ref to select from")
            case Expr.Eq(left, right) => sys.error("unexpected Eq")
            case Expr.Sel(_, _)       => sys.error("unexpected Sel")
            case Expr.Tup(_)          => sys.error("unexpected Tup")
        case _ => sys.error("expected a tuple expr")

      val (id, row) =
        val base = names.iterator.zip(valuesT).toMap
        if base.isDefinedAt("id") then base("id") -> base
        else
          val id = idCounter.toString
          idCounter += 1
          id -> (base + ("id" -> id))
      val record = Data(table, row)
      data = data.updated(id, record)
      Vector(decode(record))

  def select[T, IsScalar <: Boolean](q: SelectQuery[T, IsScalar]): Seq[T] = q match
    case SelectQuery.SelectFrom(table) =>
      data.valuesIterator.collect { case data @ Data(`table`, _) =>
        decode(data)
      }.toIndexedSeq

  def delete[T](q: DeleteQuery[T]): Unit = q match
    case DeleteQuery.DeleteFrom(schema) =>
      data = data.filterNot { case (_, Data(`schema`, _)) => true }
    case DeleteQuery.Filtered(inner, filtered) =>
      filtered match
        case Expr.Eq(Expr.Sel(Expr.Ref(id), "id"), Expr.StrLit(value)) =>
          data.get(value) match
            case Some(d) if d.table == id.table =>
              data = data - value
            case _ => ()

        case _ => sys.error("unexpected filter")
end InMemoryStore
