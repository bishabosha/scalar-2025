package ntquery

import ntquery.InsertQuery.InsertTo
import ntquery.Expr.Eq
import ntquery.Expr.Sel
import ntquery.Expr.Ref
import scala.collection.BuildFrom
import scala.collection.mutable
import scala.reflect.ClassTag

class NoteSuite extends munit.FunSuite:
  trait Interp:
    def insert[T](q: InsertQuery[T]): Seq[T]
    def select[T](q: SelectQuery[T, false]): Seq[T]
    def delete[T](q: DeleteQuery[T]): Unit

  final class InMemoryStore extends DB with Interp:
    given [Z, C: ClassTag] => BuildFrom[IArray[Z], C, IArray[C]] =
      new BuildFrom[IArray[Z], C, IArray[C]] {
        override def newBuilder(from: IArray[Z]): mutable.Builder[C, IArray[C]] =
          IArray.newBuilder[C]
        override def fromSpecific(from: IArray[Z])(it: IterableOnce[C]): IArray[C] =
          IArray.from(it)
      }

    override def run[T](q: InsertQuery[T]): T = insert(q).head

    override def run[T](q: DeleteQuery[T]): Unit = delete(q)

    override def run[T, IsScalar <: Boolean: ValueOf, Res](q: SelectQuery[T, IsScalar])(using
        RunResult[T, IsScalar] =:= Res
    ): Res =
      require(!valueOf[IsScalar])
      select(q.asInstanceOf).asInstanceOf[Res]

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
      case InsertTo(schema) => Seq.empty
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

    def select[T](q: SelectQuery[T, false]): Seq[T] = q match
      case SelectQuery.SelectFrom(table) =>
        data.valuesIterator.collect { case data @ Data(`table`, _) =>
          decode(data)
        }.toIndexedSeq

    def delete[T](q: DeleteQuery[T]): Unit = q match
      case DeleteQuery.DeleteFrom(schema) =>
        data = data.filterNot { case (_, Data(`schema`, _)) => true }
      case DeleteQuery.Filtered(inner, filtered) =>
        filtered match
          case Eq(Expr.Sel(Expr.Ref(id), "id"), Expr.StrLit(value)) =>
            data.get(value) match
              case Some(d) if d.table == id.table =>
                data = data - value
              case _ => ()

          case _ => sys.error("unexpected filter")

  test("run query on in-memory store") {
    case object Note extends Table[(id: String, title: String, content: String)]

    trait Dao(db: DB):
      def createNote(title: String, content: String) =
        db.run(
          Note.insert.values(
            (title = title, content = content)
          )
        )
      def getAll() =
        db.run(
          Note.select
        )
      def deleteNoteById(id: String): Unit =
        db.run(
          Note.delete.filter(_.id === id)
        )

    val db = InMemoryStore()
    object NoteDao extends Dao(db)

    assert(db.allRecordCount == 0)
    val note1 = NoteDao.createNote(title = "test", content = "some content")

    assert(note1.id == "0")
    assert(note1.title == "test")
    assert(note1.content == "some content")

    assert(db.allRecordCount == 1)
    val fetch = db.getData(Note, note1.id)
    assert(fetch.isDefined)
    assert(fetch.get == Map("id" -> note1.id, "title" -> "test", "content" -> "some content"))
    val all = NoteDao.getAll()
    assert(all == Seq(note1))
    assert(db.allRecordCount == 1)
    NoteDao.deleteNoteById(note1.id)
    assert(db.allRecordCount == 0)
  }
