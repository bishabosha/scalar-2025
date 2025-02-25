package ntdata

import substructural.Sub

import scala.util.chaining.*

import NamedTuple.{NamedTuple, AnyNamedTuple, Names, DropNames}
import java.time.LocalDate
import scala.util.TupledFunction
import scala.reflect.ClassTag
import DataFrame.Col
import ntdata.DataFrame.SparseArr
import ntdata.DataFrame.SparseCell
import scala.annotation.meta.field

// type InnerColumns[T] = NamedTuple.From[T] match
//   case NamedTuple[ns, vs] =>
//     NamedTuple[ns, Tuple.Map[
//       Tuple.Zip[ns, vs],
//       [X] =>> X match
//         case (n, v) => DataFrame[NamedTuple[n *: EmptyTuple, v *: EmptyTuple]]
//     ]]

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

trait NamesOf[T <: AnyNamedTuple]:
  def names: IArray[String]

object NamesOf:
  final class NamesOfNT[T <: AnyNamedTuple](ns: Tuple) extends NamesOf[T]:
    val names = ns.toIArray.map(_.asInstanceOf[String])

  transparent inline given [T <: AnyNamedTuple]: NamesOf[T] =
    new NamesOfNT[T](compiletime.constValueTuple[Names[T]])

trait ParsersOf[T <: AnyNamedTuple]:
  def parsers: IArray[DFCSVParser[?]]

object ParsersOf:
  final class ParsersOfNT[T <: AnyNamedTuple](ps: Tuple) extends ParsersOf[T]:
    val parsers = ps.toIArray.map(_.asInstanceOf[DFCSVParser[?]])

  transparent inline given [T <: AnyNamedTuple]: ParsersOf[T] =
    new ParsersOfNT[T](compiletime.summonAll[Tuple.Map[DropNames[T], DFCSVParser]])

trait DFCSVParser[T]:
  def parse(cell: String): T

object DFCSVParser:
  given DFCSVParser[Int] with
    def parse(cell: String): Int = cell.toInt
  given DFCSVParser[Long] with
    def parse(cell: String): Long = cell.toLong
  given DFCSVParser[Double] with
    def parse(cell: String): Double = cell.toDouble
  given DFCSVParser[String] with
    def parse(cell: String): String = cell
  given DFCSVParser[Boolean] with
    def parse(cell: String): Boolean = cell.toBoolean
  given DFCSVParser[LocalDate] with
    def parse(cell: String): LocalDate = LocalDate.parse(cell)

trait DFShow[T]:
  extension (t: T) def show: String

object DFShow:
  given DFShow[Int] with
    extension (t: Int) def show: String = t.toString
  given DFShow[Long] with
    extension (t: Long) def show: String = t.toString
  given DFShow[Double] with
    extension (t: Double) def show: String = t.toString
  given DFShow[String] with
    extension (t: String) def show: String = t
  given DFShow[Boolean] with
    extension (t: Boolean) def show: String = t.toString
  given DFShow[LocalDate] with
    extension (t: LocalDate) def show: String = t.toString

// trait DFEq[T]:
//   extension (t: T) def ===(other: T): Boolean

// object DFEq:
//   given DFEq[Int] with
//     extension (t: Int) def ===(other: Int): Boolean = t == other
//   given DFEq[Long] with
//     extension (t: Long) def ===(other: Long): Boolean = t == other
//   given DFEq[Double] with
//     extension (t: Double) def ===(other: Double): Boolean = t == other
//   given DFEq[String] with
//     extension (t: String) def ===(other: String): Boolean = t == other
//   given DFEq[Boolean] with
//     extension (t: Boolean) def ===(other: Boolean): Boolean = t == other
//   given DFEq[LocalDate] with
//     extension (t: LocalDate) def ===(other: LocalDate): Boolean = t == other

trait TagsOf[T <: AnyNamedTuple]:
  def tags: IArray[ClassTag[?]]
  def shows: IArray[DFShow[?]]

object TagsOf:
  final class TagsOfNT[T <: AnyNamedTuple](ts: Tuple, ss: Tuple) extends TagsOf[T]:
    val tags = ts.toIArray.map(_.asInstanceOf[ClassTag[?]])
    val shows = ss.toIArray.map(_.asInstanceOf[DFShow[?]])

  transparent inline given [T <: AnyNamedTuple]: TagsOf[T] =
    new TagsOfNT[T](
      compiletime.summonAll[Tuple.Map[DropNames[T], ClassTag]],
      compiletime.summonAll[Tuple.Map[DropNames[T], DFShow]]
    )

class DataFrame[T](
    private val cols: IArray[Col[?]],
    val len: Int,
    private val data: IArray[AnyRef]
):
  def columns[F <: AnyNamedTuple: {SubNames[T], NamesOf as ns}]
      : DataFrame[FilterNames[Names[F], T]] =
    val cBuf = IArray.newBuilder[Col[?]]
    val dBuf = IArray.newBuilder[AnyRef]
    val names = ns.names
    DataFrame.loop(cols): (col, i) =>
      if names.contains(col.name) then
        cBuf += col
        dBuf += data(i)
    DataFrame(cBuf.result(), len, dBuf.result())

  def merge[U](
      other: DataFrame[U]
  )(using NamedTuple.From[T] =:= NamedTuple.From[U]): DataFrame[T] = {
    require(cols.length == other.cols.length && cols.corresponds(other.cols)(_ == _))
    val dBuf = IArray.newBuilder[AnyRef]
    DataFrame.loop(data) { (row, i) =>
      val col = cols(i)
      cols(i).tag match
        case given ClassTag[t] =>
          if col.isStrict then {
            val colInit = row.asInstanceOf[Array[t]]
            val colRest = other.data(i).asInstanceOf[Array[t]]
            val merged = colInit ++ colRest
            dBuf += merged
          } else {
            val sparse = row.asInstanceOf[SparseArr[t]]
            val sparseRest = other.data(i).asInstanceOf[SparseArr[t]]
            val merged = SparseArr(
              sparse.chain ++ sparseRest.chain.map(cell =>
                cell.copy(untilIdx = cell.untilIdx + len)
              )
            )
            dBuf += merged
          }
    }
    DataFrame(cols, len + other.len, dBuf.result())
  }

  def withValue[F <: AnyNamedTuple: {NamesOf as ns, TagsOf as ts}](f: F)(using
      Tuple.Disjoint[Names[F], Names[NamedTuple.From[T]]] =:= true
  ): DataFrame[NamedTuple.Concat[NamedTuple.From[T], F]] =
    val vs = f.asInstanceOf[Tuple].toIArray
    val cBuf = IArray.newBuilder[Col[?]] ++= cols
    val dBuf = IArray.newBuilder[AnyRef] ++= data
    val dfShows = ts.shows
    def showOf[T](i: Int): DFShow[T] = dfShows(i).asInstanceOf[DFShow[T]]
    DataFrame.loop(ts.tags):
      case (tag @ given ClassTag[t], i) =>
        val v: t = vs(i).asInstanceOf[t]
        cBuf += Col[t](ns.names(i), isStrict = false, tag, showOf[t](i))
        dBuf += SparseArr[t](IArray(SparseCell(len, v)))
    DataFrame(cBuf.result(), len, dBuf.result())

  def withComputed[F <: AnyNamedTuple: {NamesOf as ns}](
      f: DataFrame.Ref[T] ?=> F
  )(using
      ts: TagsOf[DataFrame.StripExpr[F]]
  )(using
      Tuple.Disjoint[Names[F], Names[NamedTuple.From[T]]] =:= true,
      Tuple.IsMappedBy[DataFrame.Expr][DropNames[F]]
  ): DataFrame[NamedTuple.Concat[NamedTuple.From[T], DataFrame.StripExpr[F]]] =
    val ref = new DataFrame.Ref[T]
    val exprFuncs =
      f(using ref)
        .asInstanceOf[Tuple]
        .toIArray
        .map: e =>
          val e0 = e.asInstanceOf[DataFrame.Expr[?]]
          DataFrame.compile(this, e0)
    val names = ns.names
    val cBuf = IArray.newBuilder[Col[?]] ++= cols
    val dBuf = IArray.newBuilder[AnyRef] ++= data
    val dfShows = ts.shows
    def showOf[T](i: Int): DFShow[T] = dfShows(i).asInstanceOf[DFShow[T]]
    DataFrame.loop(ts.tags):
      case (tag @ given ClassTag[t], i) =>
        val idxToValue = exprFuncs(i).asInstanceOf[Int => t]
        cBuf += Col[t](names(i), isStrict = true, tag, showOf[t](i))
        dBuf += Array.tabulate(len)(idxToValue)
    DataFrame(cBuf.result(), len, dBuf.result())

  def collectOn[F <: AnyNamedTuple: {SubNames[T], NamesOf as ns}](using
      NamedTuple.Size[F] =:= 1
  ): DataFrame.Collected[FilterNames[Names[F], T], T] =
    import scala.collection.mutable
    val name = ns.names(0)
    val dataIdx = cols.indexWhere(_.name == name)
    val col = cols(dataIdx)

    def packColls[K: ClassTag, Z <: AnyNamedTuple](
        buckets: mutable.HashMap[K, mutable.BitSet]
    ): DataFrame.Collected[Z, T] =
      val keyBuf = Array.newBuilder[K]
      val framesBuf = IArray.newBuilder[DataFrame[T]]
      buckets.foreach { (k, v) =>
        keyBuf += k
        val cBuf = IArray.newBuilder[Col[?]]
        val dBuf = IArray.newBuilder[AnyRef]
        DataFrame.loop(cols) { (col0, i) =>
          if i == dataIdx then
            cBuf += col0.copy(isStrict = false)
            dBuf += SparseArr[K](IArray(SparseCell(v.size, k)))
          else
            col0.tag match
              case given ClassTag[u] =>
                if col0.isStrict then
                  val arr = data(i).asInstanceOf[Array[u]]
                  cBuf += col0
                  dBuf += v.toArray.map(arr)
                else
                  val sparse = data(i).asInstanceOf[SparseArr[u]]
                  val d0Buf = Array.newBuilder[u]
                  var init = 0
                  val cells = sparse.chain.iterator
                  while cells.hasNext do
                    val cell = cells.next()
                    val limit = cell.untilIdx
                    val inner = cell.value
                    var j = init
                    while j < limit do
                      if v.contains(j) then d0Buf += inner
                      j += 1
                    end while
                    init = limit
                  end while
                  cBuf += col0.copy(isStrict = true)
                  dBuf += d0Buf.result()
        }
        framesBuf += DataFrame[T](cBuf.result(), v.size, dBuf.result())
      }
      val keysData = keyBuf.result()
      val keys =
        DataFrame[Z](IArray(col.copy(isStrict = true)), keysData.length, IArray(keysData))
      DataFrame.CollectedImpl(keys, framesBuf.result())
    end packColls

    col.tag match
      case given ClassTag[t] =>
        if col.isStrict then
          val arr = data(dataIdx).asInstanceOf[IArray[t]]
          val buckets = mutable.HashMap.empty[t, mutable.BitSet]
          DataFrame.loop(arr)((elem, i) =>
            buckets.getOrElseUpdate(elem, mutable.BitSet.empty).add(i)
          )
          packColls(buckets)
        else
          val sparse = data(dataIdx).asInstanceOf[SparseArr[t]]
          val buckets = mutable.HashMap.empty[t, mutable.BitSet]
          var init = 0
          val cells = sparse.chain.iterator
          while cells.hasNext do
            val cell = cells.next()
            var i = init
            while i < cell.untilIdx do
              buckets.getOrElseUpdate(cell.value, mutable.BitSet.empty).add(i)
              i += 1
            init = cell.untilIdx
          end while
          packColls(buckets)
  end collectOn

  def show(n: Int = 10): String = {
    val sb = new StringBuilder

    if (cols.isEmpty) {
      sb ++= "Empty DataFrame"
      return sb.result()
    }

    val numRows = len
    val dataRows: IndexedSeq[IArray[String]] = (0 until math.min(n, numRows)).map { i =>
      cols
        .zip(data)
        .map((col, data) => {
          col.tag match
            case given ClassTag[t] =>
              given DFShow[t] = col.dfShow
              if col.isStrict then
                val arr = data.asInstanceOf[Array[t]]
                arr(i).show
              else
                val sparse = data.asInstanceOf[SparseArr[t]]
                val cellIdx = sparse.chain.indexWhere(_.untilIdx > i)
                sparse.chain(cellIdx).value.show
        })
    }

    // Calculate column widths
    val columnWidths: Seq[Int] = cols.zipWithIndex.map { case (col, index) =>
      val headerWidth = col.name.length
      val dataWidth = dataRows.map(_(index).length).maxOption.getOrElse(0)
      math.max(headerWidth, dataWidth) + 2 // Add padding
    }

    // Format header
    val header = cols
      .zip(columnWidths)
      .map { case (col, width) =>
        String.format(s" %-${width - 2}s ", col.name)
      }
      .mkString("│", "┆", "│")
    val headerLine = "┌" + columnWidths.map("─" * _).mkString("┬") + "┐"
    val headerSeparator = "╞" + columnWidths.map("═" * _).mkString("╪") + "╡"
    // Format data rows
    val formattedRows = dataRows.map { row =>
      row
        .zip(columnWidths)
        .map { case (value, width) =>
          String.format(s" %-${width - 2}s ", value)
        }
        .mkString("│", "┆", "│")
    }

    val footerLine = "└" + columnWidths.map("─" * _).mkString("┴") + "┘"

    // Print the formatted output
    sb ++= (headerLine) += '\n'
    sb ++= (header) += '\n'
    sb ++= (headerSeparator) += '\n'
    formattedRows.foreach(sb ++= _ += '\n')
    sb ++= (footerLine)
    sb.result()
  }

object DataFrame {

  def col[T: {Ref as ref}]: ref.type = ref

  inline def loop[T](arr: IArray[T])(inline f: (T, Int) => Unit): Unit =
    var i = 0
    val len = arr.length
    while i < len do
      f(arr(i), i)
      i += 1

  case class Col[T](name: String, isStrict: Boolean, tag: ClassTag[T], dfShow: DFShow[T])
  case class SparseCell[T](untilIdx: Int, value: T)
  case class SparseArr[T](chain: IArray[SparseCell[T]])

  trait Collected[Col <: AnyNamedTuple, T]:
    def keys: DataFrame[Col]
    def get(filter: Tuple.Head[DropNames[Col]]): Option[DataFrame[T]]
    def columns[F <: AnyNamedTuple: {SubNames[T], NamesOf}]
        : Collected[Col, FilterNames[Names[F], T]]

  private class CollectedImpl[Col <: AnyNamedTuple, T](
      val keys: DataFrame[Col],
      private val frames: IArray[DataFrame[T]]
  ) extends Collected[Col, T]:

    override def columns[F <: AnyNamedTuple: {SubNames[T], NamesOf}]
        : Collected[Col, FilterNames[Names[F], T]] =
      CollectedImpl(keys, frames.map(_.columns[F]))

    def get(filter: Tuple.Head[DropNames[Col]]): Option[DataFrame[T]] =
      val col = keys.cols(0)
      val data = keys.data(0)
      val index =
        col.tag match
          case given ClassTag[t] =>
            if col.isStrict then
              val arr = data.asInstanceOf[Array[t]]
              arr.indexOf(filter.asInstanceOf[t])
            else
              val sparse = data.asInstanceOf[SparseArr[t]]
              sparse.chain.indexWhere(_.value == filter.asInstanceOf[t])
      if index == -1 then None else Some(frames(index))

  private def compile[T](df: DataFrame[?], expr: Expr[T]): Int => T =
    expr match
      case ColRef(name) =>
        val idx = df.cols.indexWhere(_.name == name)
        val col = df.cols(idx).asInstanceOf[Col[T]]
        col.tag match
          case given ClassTag[T] =>
            if col.isStrict then
              val arr = df.data(idx).asInstanceOf[Array[T]]
              i => arr(i)
            else
              val sparse = df.data(idx).asInstanceOf[SparseArr[T]]
              i =>
                val cellIdx = sparse.chain.indexWhere(_.untilIdx > i)
                sparse.chain(cellIdx).value
        end match
      case Splice(func, args) =>
        val compiledArgs = args.map(compile(df, _))
        i => func(Tuple.fromIArray(compiledArgs.map(_(i))))

  type Func[G] = G match
    case (g => r) => Tuple.Map[g, Expr] => Expr[r]

  type In[G] <: Tuple = G match
    case (g => _) => g & Tuple

  type Out[G] = G match
    case (_ => r) => r

  type StripExpr[T <: AnyNamedTuple] =
    NamedTuple.Map[T, [X] =>> X match { case Expr[t] => t }]

  def fun[F, G](f: F)(using tf: TupledFunction[F, G])(in: Tuple.Map[In[G], Expr]): Expr[Out[G]] =
    Splice(tf.tupled(f).asInstanceOf[Tuple => Out[G]], in.toIArray.map(_.asInstanceOf[Expr[?]]))

  final case class Ref[T]() extends Selectable:
    type Fields = NamedTuple.Map[NamedTuple.From[T], Expr]
    def selectDynamic(name: String): Expr[?] = ColRef(name)

  sealed trait Expr[T]
  final case class ColRef[T](name: String) extends Expr[T]
  final case class Splice[T](func: Tuple => T, args: IArray[Expr[?]]) extends Expr[T]

  def fromCSV[T](
      path: String
  )(using
      ns: NamesOf[NamedTuple.From[T]],
      ps: ParsersOf[NamedTuple.From[T]],
      ts: TagsOf[NamedTuple.From[T]]
  ): DataFrame[T] =
    import scala.jdk.CollectionConverters.given
    import scala.collection.mutable
    val names = ns.names
    val src = java.nio.file.Files.readAllLines(java.nio.file.Paths.get(path)).asScala
    val header = src.head.split(",")
    val idxLookup = header.zipWithIndex.toMap
    assert(
      names.forall(idxLookup.contains),
      s"missing columns in CSV: ${names.filterNot(idxLookup.contains)}"
    )
    val colIdxs = names.map(idxLookup(_))
    val colBufs = ts.tags.map({ case given ClassTag[t] =>
      IArray.newBuilder[t]
    })

    for s <- src.dropInPlace(1) do
      val row0 = IArray.unsafeFromArray(s.split(","))
      require(row0.length == header.length)
      val row = colIdxs.map(row0(_))
      loop(row) { (cell, i) =>
        val parser = ps.parsers(i)
        val parsed = parser.parse(cell)
        ts.tags(i) match
          case given ClassTag[t] =>
            colBufs(i).asInstanceOf[mutable.Builder[t, IArray[t]]].addOne(parsed.asInstanceOf[t])
      }

    val cols =
      names.zip(ts.tags).zip(ts.shows).map { case ((name, tag @ given ClassTag[t]), show) =>
        Col(name, isStrict = true, tag, show.asInstanceOf[DFShow[t]])
      }
    val data = colBufs.map(_.result().asInstanceOf[AnyRef])
    DataFrame(cols, src.length, data)
}

object demo {
  type Session = (duration: Long, pulse: Long, maxPulse: Long, calories: Double)

  val df: DataFrame[Session] =
    DataFrame.fromCSV[Session]("data.csv")

  val d: DataFrame[(duration: Long)] =
    df.columns[(duration: ?)]

  val pm: DataFrame[(pulse: Long, maxPulse: Long)] =
    df.columns[(pulse: ?, maxPulse: ?)]
}

object customers {
  type Customer = (
      id: String,
      firstname: String,
      lastname: String,
      age: Int
  ) // , City,Country,Phone 1,Phone 2,Email,Subscription Date,Website

  @main def readcustomers(): Unit =
    val df: DataFrame[Customer] =
      DataFrame.fromCSV[Customer]("customers-100.csv")

    val na = df.columns[(firstname: ?, age: ?)]

    println(df.show())
    println(na.show())
}

// TODO: expression with aggregations? look at pola.rs and pokemon dataset

object bankaccs {
  import DataFrame.col

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
      .withComputed:
        (cat_computed = DataFrame.fun(hsbcCat)((col.kind, col.merchant)))
      .columns[Cols]
      .merge:
        monzo
          .withValue((acc_kind = "monzo"))
          .withComputed:
            (cat_computed = DataFrame.fun(monzoCat)((col.category, col.kind, col.name)))
          .columns[Cols]
      .merge:
        creditsuisse
          .withValue((acc_kind = "creditsuisse"))
          .withComputed:
            (cat_computed = DataFrame.fun(creditsuisseCat)((col.category, col.merchant)))
          .columns[Cols]

  val byKind = all.collectOn[(acc_kind: ?)].columns[(id: ?, cat_computed: ?)]
  val kinds = byKind.keys
  val hsbcAgg = byKind.get("hsbc").get
  val monzoAgg = byKind.get("monzo").get
  val creditsuisseAgg = byKind.get("creditsuisse").get
}
