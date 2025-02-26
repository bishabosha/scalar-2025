package ntdataframe

import java.time.LocalDate
import scala.reflect.ClassTag
import scala.util.TupledFunction
import scala.util.chaining.*

import NamedTuple.{NamedTuple, AnyNamedTuple, Names, DropNames}
import DataFrame.Col
import DataFrame.SparseArr
import DataFrame.TagsOf

import TupleUtils.*

object DataFrame:

  def showDF[T](df: DataFrame[T], n: Int = 10): String =
    if df.cols.isEmpty then return "Empty DataFrame"
    val shape = s"shape: (${df.len}, ${df.cols.size})"

    val dataRows: IndexedSeq[IArray[String]] =
      val foo = (0.until(math.min(n, df.len)))

      foo.map(i =>
        df.cols
          .zip(df.data)
          .map: (col, data) =>
            col.tag match
              case given ClassTag[t] =>
                given DFShow[t] = col.dfShow
                if col.isDense then
                  val arr = data.asInstanceOf[Array[t]]
                  arr(i).show
                else
                  val sparse = data.asInstanceOf[SparseArr[t]]
                  sparse(i).show
      )

    // Calculate column widths
    val columnWidths: Seq[Int] = df.cols.zipWithIndex.map { case (col, index) =>
      val headerWidth = col.name.length
      val dataWidth = dataRows.map(_(index).length).maxOption.getOrElse(0)
      math.max(headerWidth, dataWidth) + 2 // Add padding
    }

    // Format header
    val header = df.cols
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
    val sb = StringBuilder()
    sb ++= shape += '\n'
    sb ++= headerLine += '\n'
    sb ++= header += '\n'
    sb ++= headerSeparator += '\n'
    formattedRows.foreach(sb ++= _ += '\n')
    sb ++= footerLine
    sb.result()

  trait TagsOf[T <: AnyNamedTuple]:
    def tags: IArray[ClassTag[?]]
    def shows: IArray[DFShow[?]]

  object TagsOf:
    final class TagsOfNT[T <: AnyNamedTuple](ts: Tuple, ss: Tuple) extends TagsOf[T]:
      val tags = ts.toIArray.map(_.asInstanceOf[ClassTag[?]])
      val shows = ss.toIArray.map(_.asInstanceOf[DFShow[?]])

    transparent inline given [T <: AnyNamedTuple]: TagsOf[T] =
      TagsOfNT[T](
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

  inline def loop[T](sparse: SparseArr[T])(inline f: (T, Int) => Unit): Unit =
    var init = 0
    var i = 0
    val cells = sparse.untils.length
    while i < cells do
      val limit = sparse.untils(i)
      val cell = sparse.values(i)
      var j = init
      while j < limit do
        f(cell, i)
        j += 1
      init = limit
    end while

  case class Col[T](name: String, isDense: Boolean, tag: ClassTag[T], dfShow: DFShow[T])
  case class SparseArr[T](untils: IArray[Int], values: IArray[T]):
    def apply(i: Int): T =
      val cellIdx = untils.indexWhere(_ > i)
      values(cellIdx)
    def indexOf(value: T): Int =
      values.indexOf(value)

  trait Collected[Col <: AnyNamedTuple, T]:
    def keys: DataFrame[Col]
    def get(filter: Tuple.Head[DropNames[Col]]): Option[DataFrame[T]]
    def columns[
        F <: AnyNamedTuple: {SubNames[T], NamesOf}
    ]: Collected[Col, FilterNames[Names[F], T]]

  private class CollectedImpl[Col <: AnyNamedTuple, T](
      val keys: DataFrame[Col],
      private val frames: IArray[DataFrame[T]]
  ) extends Collected[Col, T] {
    require(keys.cols.size == 1 && keys.cols(0).isDense)

    override def columns[
        F <: AnyNamedTuple: {SubNames[T], NamesOf}
    ]: Collected[Col, FilterNames[Names[F], T]] =
      CollectedImpl(keys, frames.map(_.columns[F]))

    def get(filter: Tuple.Head[DropNames[Col]]): Option[DataFrame[T]] =
      val col = keys.cols(0)
      val data = keys.data(0)
      val index =
        col.tag match
          case given ClassTag[t] =>
            val arr = data.asInstanceOf[Array[t]]
            arr.indexOf(filter.asInstanceOf[t])
      if index == -1 then None else Some(frames(index))
  }

  private def compile[T](df: DataFrame[?], expr: Expr[T]): Int => T =
    expr match {
      case ColRef(name) =>
        val idx = df.cols.indexWhere(_.name == name)
        val col = df.cols(idx).asInstanceOf[Col[T]]
        col.tag match
          case given ClassTag[T] =>
            if col.isDense then
              val arr = df.data(idx).asInstanceOf[Array[T]]
              arr(_)
            else
              val sparse = df.data(idx).asInstanceOf[SparseArr[T]]
              sparse(_)
        end match
      case Splice(opt, args) =>
        opt(args.map(compile(df, _)))
    }

  type In[G] <: Tuple = G match
    case (g => _) => g & Tuple

  type Out[G] = G match
    case (_ => r) => r

  type StripExpr[T <: AnyNamedTuple] =
    NamedTuple.Map[T, [X] =>> X match { case Expr[t] => t }]

  def fun[R](f: => R): Expr[R] =
    Splice(_ => _ => f, IArray.empty)

  def fun[I, R](f: I => R)(arg: Expr[I]): Expr[R] =
    val opt: (IArray[Int => Any]) => Int => R = args =>
      val x1 = args(0).asInstanceOf[Int => I]
      i => f(x1(i))
    Splice(opt, IArray(arg))

  def fun[F, G](f: F)(using tf: TupledFunction[F, G])(in: Tuple.Map[In[G], Expr]): Expr[Out[G]] =
    val argExprs = in.toIArray.map(_.asInstanceOf[Expr[?]])
    type Res = Out[G]
    val opt: (IArray[Int => Any]) => Int => Res = argExprs.length match
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

  def readCSV[T](
      src: IterableOnce[String]
  )(using
      ns: NamesOf[NamedTuple.From[T]],
      ps: ParsersOf[NamedTuple.From[T]],
      ts: TagsOf[NamedTuple.From[T]]
  ): DataFrame[T] =
    readCSVCore(
      src,
      Some(ns.names),
      [T] => ps.parsers(_).asInstanceOf[DFCSVParser[T]],
      [T] => ts.shows(_).asInstanceOf[DFShow[T]],
      ts.tags
    )

  def readCSV[T](
      path: String
  )(using
      ns: NamesOf[NamedTuple.From[T]],
      ps: ParsersOf[NamedTuple.From[T]],
      ts: TagsOf[NamedTuple.From[T]]
  ): DataFrame[T] =
    import scala.jdk.CollectionConverters.given
    import java.nio.file.{Paths, Files}
    val s = Files.lines(Paths.get(path))
    try readCSV[T](s.iterator().asScala)
    finally s.close()

  def readAnyCSV(
      src: IterableOnce[String]
  ): DataFrame[Any] =
    readCSVCore(
      src,
      None,
      [T] => _ => summon[DFCSVParser[String]].asInstanceOf[DFCSVParser[T]],
      [T] => _ => summon[DFShow[String]].asInstanceOf[DFShow[T]],
      _ => reflect.classTag[String]
    )

  def readAnyCSV(
      path: String
  ): DataFrame[Any] =
    import scala.jdk.CollectionConverters.given
    import java.nio.file.{Paths, Files}
    val s = Files.lines(Paths.get(path))
    try readAnyCSV(s.iterator().asScala)
    finally s.close()

  private def readCSVCore[T](
      src: IterableOnce[String],
      ns: Option[IArray[String]],
      ps: [T] => Int => DFCSVParser[T],
      show: [T] => Int => DFShow[T],
      tag: Int => ClassTag[?]
  ): DataFrame[T] =
    import scala.collection.mutable

    val it = src.iterator
    if !it.hasNext then return DataFrame(IArray.empty, 0, IArray.empty)

    // Parse header with quote awareness
    val header = parseCSVLine(it.next())
    val colIdxs: IArray[Int] = ns match
      case Some(names) =>
        val idxLookup = header.zipWithIndex.toMap
        assert(
          names.forall(idxLookup.contains),
          s"missing columns in CSV: ${names.filterNot(idxLookup.contains)}"
        )
        names.map(idxLookup(_))
      case None =>
        IArray.from(header.indices)
    val colNames = colIdxs.map(header)

    var len = 0
    val dBufs = IArray.from(
      colNames.indices.map(i =>
        tag(i) match
          case given ClassTag[t] =>
            Array.newBuilder[t]
      )
    )

    while it.hasNext do
      val s = it.next()
      len += 1
      val row = parseCSVLine(s)
      require(
        row.length == header.length,
        s"Row has ${row.length} cells, expected ${header.length}"
      )
      val selectedCells = colIdxs.map(row(_))
      loop(selectedCells) { (cell, i) =>
        tag(i) match
          case given ClassTag[t] =>
            val parser = ps[t](i)
            val parsed = parser.parse(cell)
            dBufs(i).asInstanceOf[mutable.Builder[t, Array[t]]].addOne(parsed.asInstanceOf[t])
      }

    val cBuf = IArray.newBuilder[Col[?]]
    loop(colNames)((name, i) =>
      tag(i) match
        case tag @ given ClassTag[t] =>
          cBuf += Col(name, isDense = true, tag, show[t](i))
    )
    val data = dBufs.map[AnyRef](_.result())
    DataFrame(cBuf.result(), len, data)
  end readCSVCore

  /** Parse a CSV line respecting quoted cells that may contain commas
    */
  private def parseCSVLine(line: String): IArray[String] =
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
  end parseCSVLine
end DataFrame

class DataFrame[T](
    private val cols: IArray[Col[?]],
    val len: Int,
    private val data: IArray[AnyRef]
):
  def columns[
      F <: AnyNamedTuple: {SubNames[T], NamesOf as ns}
  ]: DataFrame[FilterNames[Names[F], T]] =
    columnsRaw(ns.names)

  def columns(names: String*): DataFrame[Any] =
    val missing = names.filterNot(n => cols.exists(_.name == n))
    require(
      missing.isEmpty,
      s"unknown column: ${missing.mkString(", ")}"
    )
    columnsRaw(IArray.from(names))

  def columnLabels: DataFrame[String] =
    // TODO: introduce series type?
    val col = Col(name = "label", isDense = true, reflect.classTag[String], summon[DFShow[String]])
    val data = cols.unsafeArray.map(_.name)
    DataFrame(IArray(col), cols.size, IArray(data))

  private def columnsRaw[U](names: IArray[String]): DataFrame[U] =
    val cBuf = IArray.newBuilder[Col[?]]
    val dBuf = IArray.newBuilder[AnyRef]
    DataFrame.loop(names): (name, _) =>
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
          if col.isDense then {
            val colInit = row.asInstanceOf[Array[t]]
            val colRest = other.data(i).asInstanceOf[Array[t]]
            val merged = colInit ++ colRest
            dBuf += merged
          } else {
            val sparse = row.asInstanceOf[SparseArr[t]]
            val sparseRest = other.data(i).asInstanceOf[SparseArr[t]]
            val merged = SparseArr(
              sparse.untils ++ sparseRest.untils.map(_ + len),
              sparse.values ++ sparseRest.values
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
        cBuf += Col[t](ns.names(i), isDense = false, tag, showOf[t](i))
        dBuf += SparseArr[t](IArray(len), IArray(v))
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
        cBuf += Col[t](names(i), isDense = true, tag, showOf[t](i))
        dBuf += Array.tabulate[t](len)(idxToValue)
    DataFrame(cBuf.result(), len, dBuf.result())
  end withComputed

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
            cBuf += col0.copy(isDense = false)
            dBuf += SparseArr[K](IArray(v.size), IArray(k))
          else
            col0.tag match
              case given ClassTag[u] =>
                if col0.isDense then
                  val arr = data(i).asInstanceOf[Array[u]]
                  cBuf += col0
                  dBuf += v.toArray.map(arr)
                else
                  val sparse = data(i).asInstanceOf[SparseArr[u]]
                  val d0Buf = Array.newBuilder[u]
                  DataFrame.loop(sparse): (cell, j) =>
                    if v.contains(j) then d0Buf += cell
                  cBuf += col0.copy(isDense = true)
                  dBuf += d0Buf.result()
        }
        framesBuf += DataFrame[T](cBuf.result(), v.size, dBuf.result())
      }
      val keysData = keyBuf.result()
      val keys =
        DataFrame[Z](IArray(col.copy(isDense = true)), keysData.length, IArray(keysData))
      DataFrame.CollectedImpl(keys, framesBuf.result())
    end packColls

    col.tag match
      case given ClassTag[t] =>
        if col.isDense then
          val arr = data(dataIdx).asInstanceOf[IArray[t]]
          val buckets = mutable.HashMap.empty[t, mutable.BitSet]
          DataFrame.loop(arr)((elem, i) =>
            buckets.getOrElseUpdate(elem, mutable.BitSet.empty).add(i)
          )
          packColls(buckets)
        else
          val sparse = data(dataIdx).asInstanceOf[SparseArr[t]]
          val buckets = mutable.HashMap.empty[t, mutable.BitSet]
          DataFrame.loop(sparse): (cell, i) =>
            buckets.getOrElseUpdate(cell, mutable.BitSet.empty).add(i)
          packColls(buckets)
        end if
    end match
  end collectOn

  def show(n: Int = 10): String = DataFrame.showDF(this, n)

end DataFrame
