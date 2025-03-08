package ntquery

import scala.reflect.ClassTag
import scala.collection.BuildFrom
import scala.collection.mutable
import scala.annotation.targetName
import scala.collection.concurrent
import scala.jdk.CollectionConverters.given
import java.util.concurrent.atomic.AtomicInteger
import java.util.concurrent.atomic.AtomicReference
import scala.util.Using
import javax.xml.transform.Source

trait Interp:
  def insert[T](q: InsertQuery[T]): Seq[T]
  def select[T, S <: Shape](q: SelectQuery[T, S]): Seq[T]
  def delete[T](q: DeleteQuery[T]): Unit

object LogBasedStore:
  given [From <: IArray[Any], C: ClassTag] => BuildFrom[From, C, IArray[C]]:
    override def newBuilder(from: From): mutable.Builder[C, IArray[C]] =
      IArray.newBuilder[C]
    override def fromSpecific(from: From)(it: IterableOnce[C]): IArray[C] =
      IArray.from(it)

final class LogBasedStore extends DB with Interp:
  import LogBasedStore.given

  override def run[T](q: InsertQuery[T]): T = insert(q).head
  override def run[T](q: DeleteQuery[T]): Unit = delete(q)
  override def run[T](q: SelectQuery[T, Shape.Vec]): Seq[T] = select(q)
  @targetName("runSingle")
  override def run[T](q: SelectQuery[T, Shape.Scalar]): T = select(q).head

  private val idCounter = AtomicInteger(0)
  private val data =
    java.util.concurrent.ConcurrentHashMap[String, Data[?]]().asScala
  private val tables =
    java.util.concurrent.ConcurrentHashMap[String, Table[?]]().asScala
  private val log = AtomicReference(List.empty[String])

  def refreshTables(tables: Table[?]*): Unit =
    tables.foreach(t => this.tables.update(t.name, t))
    var latestId = ""
    println(s"[LogBasedStore] Looking for log in ${new java.io.File(".").getCanonicalPath}")
    val loadRes = Using(scala.io.Source.fromFile("log.txt")) { source =>
      println(s"[LogBasedStore] Loading log lines from ${source.descr}")
      source.getLines().foreach { line =>
        val optId = runEvent(decodeEvent(line))
        if optId != null then latestId = optId
      }
    }
    if loadRes.isFailure then
      val err = loadRes.failed.get
      println(s"[LogBasedStore] Failed to load log: ${err}\n${err.getStackTrace.mkString("\n")}")
    if latestId.nonEmpty then idCounter.set(latestId.toInt + 1)

  def logEvent(e: Event): Unit =
    val logRow = serEvent(e)
    log.updateAndGet(es => logRow :: es)

  def flushLogToDisk(): Unit =
    Using
      .Manager: use =>
        val fw = use(java.io.FileWriter("log.txt", true))
        val pw = use(java.io.PrintWriter(fw, true))
        val events = log.getAndUpdate(_ => Nil).reverse
        println(s"[LogBasedStore] Flushing ${events.size} events to disk")
        events.foreach(pw.println)
      .get

  def peekLog: List[String] = log.get.reverse

  def runEvent(e: Event): String | Null = e match
    case Event.Insert(id, record) =>
      println(s"[LogBasedStore] Inserting $id")
      data.update(id, record)
      id
    case Event.Delete(id) =>
      println(s"[LogBasedStore] Deleting $id")
      data.remove(id)
      null

  case class Data[T](table: Table[T], rec: Map[String, String])

  enum Event:
    case Insert(id: String, record: Data[?])
    case Delete(id: String)

  inline val BEGIN = '\u0001'
  inline val SPLIT_PAIR = '\u0002'
  inline val SPLIT = '\u0003'
  inline val EMPTY = '\u0004'
  inline val EMPTY_S = "\u0004"

  def escape(s: String): String =
    if s.isEmpty then EMPTY_S
    else
      s
        .replace("\n", "\\n")
        .replace("\u0001", "\\u0001")
        .replace("\u0002", "\\u0002")
        .replace("\u0003", "\\u0003")
        .replace("\u0004", "\\u0004")

  def unescape(s: String): String =
    if s == EMPTY_S then ""
    else
      s.replace("\\n", "\n")
        .replace("\\u0001", "\u0001")
        .replace("\\u0002", "\u0002")
        .replace("\\u0003", "\u0003")
        .replace("\\u0004", "\u0004")

  def serEvent(e: Event): String = e match
    case Event.Insert(id, record) =>
      "I" + escape(id) + BEGIN + escape(record.table.name) + BEGIN + record.rec
        .map((k, v) => escape(k) + SPLIT_PAIR + escape(v))
        .mkString(SPLIT.toString)
    case Event.Delete(id) =>
      "D" + escape(id)

  def decodeEvent(e: String): Event = e(0) match
    case 'I' =>
      val parts = e.substring(1).split(BEGIN)
      val id = unescape(parts(0))
      val tableId = unescape(parts(1))
      val rec = parts(2)
        .split(SPLIT)
        .map { p =>
          val kv = p.split(SPLIT_PAIR)
          unescape(kv(0)) -> unescape(kv(1))
        }
        .toMap
      val table = tables(tableId)
      Event.Insert(id, Data(table, rec))
    case 'D' => Event.Delete(unescape(e.substring(1)))

  def allRecordCount = data.size
  def getData[T](table: Table[T], id: String): Option[Map[String, String]] =
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
              case ColSchema.Str => data.rec(n)
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
          val id = idCounter.getAndIncrement().toString
          id -> (base + ("id" -> id))
      val record = Data(table, row)
      tables.update(table.name, table)
      data.update(id, record)
      logEvent(Event.Insert(id, record))
      Vector(decode(record))

  def select[T, S <: Shape](q: SelectQuery[T, S]): Seq[T] = q match
    case SelectQuery.SelectFrom(table) =>
      println(s"[LogBasedStore] Selecting from $table @ ${System.identityHashCode(data)}")
      val elems = data.valuesIterator.collect { case data @ Data(`table`, _) =>
        decode(data)
      }.toIndexedSeq

      data.valuesIterator
        .collect({ case d @ Data(t, _) if t != table => d })
        .foreach(d => println(s"[LogBasedStore] Skipping $d"))

      elems

  def delete[T](q: DeleteQuery[T]): Unit = q match
    case DeleteQuery.DeleteFrom(table) =>
      data.filterInPlace((_, v) => !(v.table == table))
    case DeleteQuery.Filtered(inner, filtered) =>
      filtered match
        case Expr.Eq(Expr.Sel(Expr.Ref(id), "id"), Expr.StrLit(value)) =>
          data.filterInPlace: (_, v) =>
            val dropped = v.table == inner.table && v.rec("id") == value
            if dropped then logEvent(Event.Delete(value))
            !dropped

        case _ => sys.error("unexpected filter")
end LogBasedStore
