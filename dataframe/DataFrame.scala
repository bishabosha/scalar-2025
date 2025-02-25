package ntdataframe

import java.time.LocalDate
import scala.reflect.ClassTag
import scala.util.TupledFunction
import scala.util.chaining.*

import NamedTuple.{NamedTuple, AnyNamedTuple, Names, DropNames}
import DataFrame.Col
import DataFrame.SparseArr
import DataFrame.SparseCell
import DataFrame.TagsOf

import TupleUtils.*

class DataFrame[T](
    private val cols: IArray[Col[?]],
    val len: Int,
    private val data: IArray[AnyRef]
):
  def columns[F <: AnyNamedTuple: {SubNames[T], NamesOf as ns}]
      : DataFrame[FilterNames[Names[F], T]] =
    val cBuf = IArray.newBuilder[Col[?]]
    val dBuf = IArray.newBuilder[AnyRef]
    DataFrame.loop(ns.names): (name, _) =>
      val i = cols.indexWhere(_.name == name)
      cBuf += cols(i)
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
        dBuf += Array.tabulate[t](len)(idxToValue)
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
      case Splice(opt, args) =>
        opt(args.map(compile(df, _)))

  // type Func[G] = G match
  //   case (g => r) => Tuple.Map[g, Expr] => Expr[r]

  type In[G] <: Tuple = G match
    case (g => _) => g & Tuple

  type Out[G] = G match
    case (_ => r) => r

  type StripExpr[T <: AnyNamedTuple] =
    NamedTuple.Map[T, [X] =>> X match { case Expr[t] => t }]

  def fun[F, G](f: F)(using tf: TupledFunction[F, G])(in: Tuple.Map[In[G], Expr]): Expr[Out[G]] =
    val argExprs = in.toIArray.map(_.asInstanceOf[Expr[?]])
    type Res = Out[G]
    val opt: (IArray[Int => Any]) => Int => Res = argExprs.length match
      case 0 =>
        val f0 = f.asInstanceOf[() => Res]
        _ => _ => f0()
      case 1 =>
        val f1 = f.asInstanceOf[Any => Res]
        args =>
          val x1 = args(0)
          i => f1(x1(i))
      case 2 =>
        val f2 = f.asInstanceOf[(Any, Any) => Res]
        args =>
          val x1 = args(0)
          val x2 = args(1)
          i => f2(x1(i), x2(i))
      case 3 =>
        val f3 = f.asInstanceOf[(Any, Any, Any) => Res]
        args =>
          val x1 = args(0)
          val x2 = args(1)
          val x3 = args(2)
          i => f3(x1(i), x2(i), x3(i))
      case _ =>
        val func = tf.tupled(f).asInstanceOf[Tuple => Res]
        args => i => func(Tuple.fromIArray(args.map(_(i))))
    Splice(opt, argExprs)

  final case class Ref[T]() extends Selectable:
    type Fields = NamedTuple.Map[NamedTuple.From[T], Expr]
    def selectDynamic(name: String): Expr[?] = ColRef(name)

  sealed trait Expr[T]
  final case class ColRef[T](name: String) extends Expr[T]
  final case class Splice[T](opt: IArray[Int => Any] => Int => T, args: IArray[Expr[?]])
      extends Expr[T]

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

    // Parse header with quote awareness
    val header = parseCSVLine(src.head)
    val idxLookup = header.zipWithIndex.toMap

    assert(
      names.forall(idxLookup.contains),
      s"missing columns in CSV: ${names.filterNot(idxLookup.contains)}"
    )
    val colIdxs = names.map(idxLookup(_))
    var len = 0
    val colBufs = ts.tags.map({ case given ClassTag[t] =>
      IArray.newBuilder[t]
    })

    for s <- src.dropInPlace(1) do
      val row = parseCSVLine(s)
      require(
        row.length == header.length,
        s"Row has ${row.length} cells, expected ${header.length}"
      )
      val selectedCells = colIdxs.map(row(_))
      loop(selectedCells) { (cell, i) =>
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

  def exploreCSV(
      path: String
  ): DataFrame[Any] =
    import scala.jdk.CollectionConverters.given
    import scala.collection.mutable
    val src = java.nio.file.Files.readAllLines(java.nio.file.Paths.get(path)).asScala

    // Parse header with quote awareness
    val header = parseCSVLine(src.head)
    val idxLookup = header.zipWithIndex.toMap

    val colBufs = header.map(_ => IArray.newBuilder[String])

    for s <- src.dropInPlace(1) do
      val row = parseCSVLine(s)
      require(
        row.length == header.length,
        s"Row has ${row.length} cells, expected ${header.length}"
      )
      loop(row) { (cell, i) =>
        colBufs(i).addOne(cell)
      }

    val cols =
      header.map { name =>
        Col(name, isStrict = true, reflect.classTag[String], summon[DFShow[String]])
      }
    val data = colBufs.map(_.result().asInstanceOf[AnyRef])
    DataFrame(cols, src.length, data)

  /** Parse a CSV line respecting quoted cells that may contain commas
    */
  private def parseCSVLine(line: String): IArray[String] = {
    import scala.collection.mutable
    val cells = IArray.newBuilder[String]
    var i = 0
    val len = line.length
    var currentCell = StringBuilder()
    var insideQuotes = false

    while i < len do
      val c = line(i)

      if c == '"' then
        // Check if this is an escaped quote (double quote inside a quoted section)
        if insideQuotes && i + 1 < len && line(i + 1) == '"' then
          currentCell += '"'
          i += 2 // Skip both quote chars
        else
          // Toggle quote mode
          insideQuotes = !insideQuotes
          i += 1
        end if
      else if c == ',' && !insideQuotes then
        // End of cell
        cells += currentCell.result()
        currentCell = StringBuilder()
        i += 1
      else
        // Regular character
        currentCell += c
        i += 1
      end if
    end while

    // Add the last cell
    cells += currentCell.result()

    // Convert to IArray
    cells.result()
  }
}
