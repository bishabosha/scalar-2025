package ntdataframe

import java.time.LocalDate
import scala.reflect.ClassTag
import scala.util.TupledFunction
import scala.util.chaining.*
import scala.collection.mutable

import NamedTuple.{NamedTuple, AnyNamedTuple, Names, DropNames}
import DataFrame.Col
import DataFrame.SparseArr
import DataFrame.TagsOf

import TupleUtils.*
import scala.util.boundary, boundary.break
import scala.deriving.Mirror
import scala.collection.mutable.ArrayBuilder
import ntdataframe.DataFrame.Single
import ntdataframe.DataFrame.IsExprRes
import ntdataframe.DataFrame.IsConcat
import ntdataframe.DataFrame.ExprFunc

object DataFrame:

  def showDF[T](df: DataFrame[T], n: Int = 10): String =
    if df.cols.isEmpty then return "Empty DataFrame"
    val shape = s"shape: (${df.len}, ${df.cols.size})"

    val dataRows: IndexedSeq[IArray[String]] =
      (0 until math.min(n, df.len)).map { i =>
        df.cols
          .zip(df.data)
          .map: (col, data) =>
            col.tag match
              case given ClassTag[t10] =>
                given DFShow[t10] = col.dfShow
                if col.isDense then
                  val arr = data.asInstanceOf[Array[t10]]
                  arr(i)
                    .ensuring(_ != null, s"de null at $i, ${col.name}, show: ${col.dfShow}")
                    .show
                else
                  val sparse = data.asInstanceOf[SparseArr[t10]]
                  sparse(i)
                    .ensuring(_ != null, s"sp null at $i, ${col.name}")
                    .show
      }

    // Calculate column widths
    val columnWidths: Seq[Int] = df.cols.zipWithIndex.map: (col, index) =>
      val headerWidth = col.name.length
      val dataWidth = dataRows.map(_(index).length).maxOption.getOrElse(0)
      math.max(headerWidth, dataWidth) + 2 // Add padding

    // Format header
    val header = df.cols
      .zip(columnWidths)
      .map: (col, width) =>
        String.format(s" %-${width - 2}s ", col.name)
      .mkString("│", "┆", "│")
    val headerLine = "┌" + columnWidths.map("─" * _).mkString("┬") + "┐"
    val headerSeparator = "╞" + columnWidths.map("═" * _).mkString("╪") + "╡"
    // Format data rows
    val formattedRows = dataRows.map { row =>
      row
        .zip(columnWidths)
        .map: (value, width) =>
          String.format(s" %-${width - 2}s ", value)
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
  end showDF

  trait TagsOf[T]:
    def names: IArray[String]
    def tags: IArray[ClassTag[?]]
    def shows: IArray[DFShow[?]]

  object TagsOf:
    final class TagsOfNT[T](val names: IArray[String], ts: Tuple, ss: Tuple) extends TagsOf[T]:
      val tags = ts.toIArray.map(_.asInstanceOf[ClassTag[?]])
      val shows = ss.toIArray.map(_.asInstanceOf[DFShow[?]])

    transparent inline given [T: NamesOf as ns]: TagsOf[T] =
      TagsOfNT[T](
        ns.names,
        compiletime.summonAll[Tuple.Map[DropNames[NamedTuple.From[T]], ClassTag]],
        compiletime.summonAll[Tuple.Map[DropNames[NamedTuple.From[T]], DFShow]]
      )

  def col[T: {Ref as ref}]: ref.type = ref
  def group[D <: AnyNamedTuple, T](using ref: GRef[D, T]): ref.type = ref

  final class GRef[D <: AnyNamedTuple, T]:
    def key: NamedTuple.Map[D, GExpr] = Tuple(GKeys()).asInstanceOf
    def size: GExpr[Int] = GCount

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
        f(cell, j)
        j += 1
      i += 1
      init = limit
    end while

  private inline def binarySearch[T](arr: IArray[T])(inline upperBoundedBy: T => Boolean): Int =
    var low = 0
    var high = arr.length - 1
    var result = -1 // Default: not found

    while low <= high do
      val mid = low + ((high - low) / 2)
      if upperBoundedBy(arr(mid)) then
        result = mid
        high = mid - 1
      else low = mid + 1
    result
  end binarySearch

  def check[T](sparse: SparseArr[T]): Unit =
    require(
      sparse.untils.length == sparse.values.length,
      s"inconsistent sparse array, untils: ${sparse.untils.length}, values: ${sparse.values.length}"
    )

  inline def loopRanges[T](sparse: SparseArr[T])(inline f: (T, Int, Int) => Unit): Unit =
    check(sparse)
    var init = 0
    var i = 0
    val cells = sparse.untils.length
    while i < cells do
      val limit = sparse.untils(i)
      val cell = sparse.values(i)
      f(cell, init, limit)
      i += 1
      init = limit
    end while

  case class Col[T](name: String, isDense: Boolean, tag: ClassTag[T], dfShow: DFShow[T])
  case class SparseArr[T](untils: IArray[Int], values: IArray[T]):
    self =>
    def size: Int =
      check(self)
      if untils.isEmpty then 0 else untils.last

    def apply(i: Int): T =
      check(self)
      val cellIdx = binarySearch(untils)(_ > i)
      if i < 0 || cellIdx == -1 then throw IndexOutOfBoundsException(s"index $i out of bounds")
      values(cellIdx)

    def slice(startIdx: Int, untilIdx: Int)(using ClassTag[T]): SparseArr[T] =
      check(self)
      val uBuf = IArray.newBuilder[Int]
      val dBuf = IArray.newBuilder[T]
      sliceTo(startIdx, untilIdx, 0, uBuf, dBuf)
      SparseArr(
        uBuf.result(),
        dBuf.result()
      )

    type BuilderOf[T] = scala.collection.mutable.Builder[T, IArray[T]]
    private[SparseArr] def sliceTo(
        startIdx: Int,
        untilIdx: Int,
        offset: Int,
        uBuf: BuilderOf[Int],
        dBuf: BuilderOf[T]
    )(using ClassTag[T]): Unit =
      val len = untilIdx - startIdx
      boundary:
        loop(untils): (limit, i) =>
          if limit > startIdx || limit >= untilIdx then
            if limit >= untilIdx then
              uBuf += offset + len
              dBuf += values(i)
              break()
            else
              uBuf += offset + (limit - startIdx)
              dBuf += values(i)

    override def toString(): String =
      check(self)
      val sb = StringBuilder()
      sb ++= "SparseArr("
      var seen = 0
      loopRanges(self) { (cell, from, until) =>
        if seen > 0 then sb ++= ", "
        sb ++= s"$cell[$from..<$until]"
        seen += 1
      }
      sb ++= ")"
      sb.result()
  end SparseArr

  object SparseArr:
    class Builder[T: ClassTag](len: Int):
      private val untils = IArray.newBuilder[Int]
      private val values = IArray.newBuilder[T]
      private var init = 0

      def copyFromSparse(sparse: SparseArr[T], from: Int, until: Int): Unit =
        sparse.sliceTo(from, until, init, untils, values)
        init += until - from

      def result: SparseArr[T] =
        assert(init == len, s"expected $len elements, got $init")
        SparseArr(untils.result(), values.result()).tap(check)

  trait GroupBy[A <: AnyNamedTuple, T]:
    def keys: DataFrame[A]
    def get(filter: Tuple.Head[DropNames[A]]): Option[DataFrame[T]]
    def columns[Exprs <: Tuple](
        f: DataFrame.Ref[T] ?=> Exprs
    )[Ls <: Tuple: {DataFrame.IsExprLabels[Exprs], SubNames[T]}]: GroupBy[A, FilterNames[Ls, T]]
    def agg[F <: AnyNamedTuple](
        f: DataFrame.GRef[A, T] ?=> F
    )[R <: AnyNamedTuple: {IsGExprRes[F], TagsOf}]: DataFrame[R]

  private class GroupByImpl[N <: String, V: ClassTag, T](
      upstream: DataFrame[?],
      mask: Either[
        mutable.HashMap[V, mutable.BitSet],
        mutable.HashMap[V, mutable.ArrayBuffer[Range]]
      ], // TODO: convert to Array. (so same order as keys)
      keyCol: Int,
      val keys: DataFrame[Single[N, V]],
      private val dCols: IArray[Col[?]]
  ) extends GroupBy[Single[N, V], T] {
    type Index = V

    require(keys.cols.size == 1 && keys.cols(0).isDense)

    override def columns[Exprs <: Tuple](
        f: Ref[T] ?=> Exprs
    )[Ls <: Tuple: {DataFrame.IsExprLabels[Exprs], SubNames[T]}]
        : GroupBy[Single[N, V], FilterNames[Ls, T]] =
      val strings =
        f(using DataFrame.Ref[T]()).toIArray.map(_.asInstanceOf[DataFrame.ColRef[?, ?]].name)
      val set = strings.toSet
      val dCols0 = dCols.filter(c => set(c.name))
      val keyCol0 =
        if keyCol < 0 then -1
        else
          val needle = dCols(keyCol).name
          dCols0.indexWhere(_.name == needle)
      GroupByImpl(upstream.columnsRaw(strings), mask, keyCol0, keys, dCols0)

    def agg[F <: AnyNamedTuple](
        f: DataFrame.GRef[Single[N, V], T] ?=> F
    )[R <: AnyNamedTuple: {IsGExprRes[F], TagsOf as ts}]: DataFrame[R] =
      val dcols = ts.names
        .lazyZip(ts.tags)
        .lazyZip(ts.shows)
        .map: (name, tag, show) =>
          Col(name, isDense = true, tag, show.asInstanceOf)
      val gexprs =
        f(using GRef[Single[N, V], T]).asInstanceOf[Tuple].toIArray.map(_.asInstanceOf[GExpr[?]])
      val dBuf = new Array[AnyRef](dcols.length)

      def materializeExpr[T: ClassTag](col: Col[T], gexpr: GExpr[T]): Array[T] =
        gexpr match
          case GKeys() =>
            assert(keys.cols(0).tag == reflect.classTag[T])
            keys.data(0).asInstanceOf[Array[T]]
          case GCount =>
            val len = keys.len
            val keyData = keys.data(0).asInstanceOf[Array[V]]
            val arr = new Array[Int](len)
            val addFreq: ExprAction = mask match
              case Left(buckets)  => i => arr(i) = buckets(keyData(i)).size
              case Right(buckets) => i => arr(i) = buckets(keyData(i)).map(_.size).sum
            var i = 0
            while i < len do
              addFreq(i)
              i += 1
            arr
      end materializeExpr

      loop(dCols): (col, i) =>
        col.tag match
          case given ClassTag[t1] =>
            val gexpr: GExpr[t1] = gexprs(i).asInstanceOf[GExpr[t1]]
            dBuf(i) = materializeExpr(col, gexpr)
      DataFrame[R](dcols, keys.len, IArray.unsafeFromArray(dBuf))
    end agg

    def get(filter: Index): Option[DataFrame[T]] =
      materialize(filter)

    private def materialize(key: V): Option[DataFrame[T]] =
      mask match
        case Left(buckets) =>
          buckets.get(key).map(materializeDense(key, _))
        case Right(buckets) =>
          buckets.get(key).map(materializeSparse(key, _))

    private def materializeDense(
        k: V,
        buckets: mutable.BitSet
    ): DataFrame[T] =
      val len = buckets.size
      val dBuf = IArray.newBuilder[AnyRef]
      var vArr: Array[Int] = buckets.toArray
      val data = upstream.data
      DataFrame.loop(upstream.cols) { (col, i) =>
        if i == keyCol then dBuf += SparseArr[V](IArray(len), IArray(k))
        else
          col.tag match
            case given ClassTag[u] =>
              if col.isDense then
                val arr = data(i).asInstanceOf[Array[u]]
                dBuf += vArr.map(arr(_))
              else
                val sparse = data(i).asInstanceOf[SparseArr[u]]
                dBuf += vArr.map(sparse(_))
      }
      DataFrame[T](dCols, len, dBuf.result())

    private def materializeSparse(
        k: V,
        buckets: mutable.ArrayBuffer[Range]
    ): DataFrame[T] =
      val len = buckets.map(_.size).sum
      val dBuf = IArray.newBuilder[AnyRef]
      val data = upstream.data
      DataFrame.loop(upstream.cols) { (col, i) =>
        if i == keyCol then dBuf += SparseArr[V](IArray(len), IArray(k))
        else
          col.tag match
            case given ClassTag[u] =>
              if col.isDense then
                val arr = data(i).asInstanceOf[Array[u]]
                dBuf += {
                  val col = new Array[u](len)
                  var idx = 0
                  for r <- buckets do
                    Array.copy(arr, r.start, col, idx, r.size)
                    idx += r.size
                  col
                }
              else
                val sparse = data(i).asInstanceOf[SparseArr[u]]
                dBuf += {
                  if buckets.isEmpty then SparseArr[V](IArray.empty, IArray.empty)
                  else
                    val buf = SparseArr.Builder(len)
                    for r <- buckets do buf.copyFromSparse(sparse, r.start, r.end)
                    buf.result
                }
      }
      DataFrame[T](dCols, len, dBuf.result())
  }

  @FunctionalInterface
  trait ExprFunc[T]:
    def apply(i: Int): T

  @FunctionalInterface
  trait ExprAction:
    def apply(i: Int): Unit

  private def compile[T](df: DataFrame[?], expr: Expr[?, T]): ExprFunc[T] =
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

  def fun[R](f: => R): Expr[Any, R] =
    Splice(_ => _ => f, IArray.empty)

  def fun[I, R](f: I => R)(arg: Expr[?, I]): Expr[Any, R] =
    val opt: (IArray[ExprFunc[?]] => ExprFunc[R]) = args =>
      val x1 = args(0).asInstanceOf[ExprFunc[I]]
      i => f(x1(i))
    Splice(opt, IArray(arg))

  def fun[F, G](f: F)(using
      tf: TupledFunction[F, G]
  )(in: Tuple.Map[In[G], [X] =>> Expr[?, X]]): Expr[Any, Out[G]] =
    val argExprs = in.toIArray.map(_.asInstanceOf[Expr[?, ?]])
    type Res = Out[G]
    val opt: (IArray[ExprFunc[?]]) => ExprFunc[Res] = argExprs.length match
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

  type StripExpr[T <: AnyNamedTuple] = InverseMapNT[T, [X] =>> Expr[?, X]]
  type StripGExpr[T <: AnyNamedTuple] = InverseMapNT[T, GExpr]
  type IsExprs[F <: AnyNamedTuple] = F match
    case NamedTuple[_, vs] => vs <:< Tuple.Map[vs, [X] =>> DataFrame.Expr[?, ?]]
  type IsGExprs[F <: AnyNamedTuple] =
    F <:< NamedTuple.Map[F, [X] =>> DataFrame.GExpr[?]]
  type GExprTags[F <: AnyNamedTuple] =
    Tuple.Map[DropNames[F], [X] =>> X match { case DataFrame.GExpr[t] => t }]
  type GExprRes[F <: AnyNamedTuple] =
    NamedTuple.Map[F, [X] =>> X match { case DataFrame.GExpr[t] => t }]
  type ExprRes[F <: AnyNamedTuple] =
    NamedTuple.Map[F, [X] =>> X match { case DataFrame.Expr[?, t] => t }]
  type ColExprName[F <: ColExpr] = F match
    case ColRef[n, ?] => n
  type ColExprType[F <: ColExpr] = F match
    case ColRef[?, t] => t
  type ColName[F <: ColExpr] = [T <: String] =>> T <:< ColExprName[F]
  type ColType[F <: ColExpr] = [T] =>> T <:< ColExprType[F]
  type Key[Col <: String, T] = [K <: AnyNamedTuple] =>> K <:< FilterName[Col, T]
  type IsGExprRes[F <: AnyNamedTuple] = [T <: AnyNamedTuple] =>> T <:< GExprRes[F]
  type IsExprRes[F <: AnyNamedTuple] = [T <: AnyNamedTuple] =>> T <:< ExprRes[F]
  trait IsConcat0[T <: AnyNamedTuple, R <: AnyNamedTuple, C <: AnyNamedTuple]
  object IsConcat0:
    given [T <: AnyNamedTuple, R <: AnyNamedTuple, N <: Tuple, V <: Tuple]
      => (Tuple.Disjoint[Names[T], Names[R]] =:= true)
      => (
          N <:< Tuple.Concat[Names[T], Names[R]],
          V <:< Tuple.Concat[DropNames[T], DropNames[R]]
    ) => IsConcat0[T, R, NamedTuple[N, V]]()
  type IsConcat[T <: AnyNamedTuple, R <: AnyNamedTuple] =
    [C <: AnyNamedTuple] =>> IsConcat0[T, R, C]
  type IsGExprTags[F <: AnyNamedTuple] = [T <: Tuple] =>> T <:< GExprTags[F]
  type ExprLabels[Exprs <: Tuple] =
    Tuple.Map[Exprs, [E] =>> E match { case DataFrame.ColRef[n, ?] => n }]
  type IsExprLabels[Exprs <: Tuple] = [Ls <: Tuple] =>> Ls <:< ExprLabels[Exprs]

  final case class Ref[T]() extends Selectable:
    type Fields = Project[NamedTuple.From[T], ColRef]
    def selectDynamic(name: String): ColRef[?, ?] = ColRef(name)

  sealed trait Column[T]
  sealed trait Expr[L, T]
  sealed trait GExpr[T]

  case object GCount extends GExpr[Int]
  final case class GKeys[T]() extends GExpr[T]

  sealed trait ColExpr extends Expr[?, ?]:
    def name: String
  final case class ColRef[L, T](name: String) extends Expr[L, T] with ColExpr:
    inline def * : this.type *: EmptyTuple = Tuple(this)

  final case class Splice[T](opt: IArray[ExprFunc[?]] => ExprFunc[T], args: IArray[Expr[?, ?]])
      extends Expr[Any, T]

  private given [From <: IArray[Any], T: ClassTag]: scala.collection.BuildFrom[From, T, IArray[T]]
  with
    def fromSpecific(from: From)(it: IterableOnce[T]): IArray[T] = IArray.from(it)
    def newBuilder(from: From): scala.collection.mutable.Builder[T, IArray[T]] =
      IArray.newBuilder[T]

  type Single[N <: String, T] = NamedTuple[N *: EmptyTuple, T *: EmptyTuple]
  def column[N <: String: ValueOf, T: {ClassTag, DFShow}](
      data: Single[N, IterableOnce[T]]
  ): DataFrame[Single[N, T]] =
    val col = Col(valueOf[N], isDense = true, summon[ClassTag[T]], summon[DFShow[T]])
    val data0 = data.asInstanceOf[IterableOnce[T] *: EmptyTuple](0).iterator.toArray
    DataFrame(IArray(col), data0.length, IArray(data0))

  def from[T: {Mirror.ProductOf as m, TagsOf as ts}](
      data: IterableOnce[T]
  ): DataFrame[T] =
    val it = data.iterator
    val cols = ts.names
      .lazyZip(ts.tags)
      .lazyZip(ts.shows)
      .map((name, tag, show) => Col(name, isDense = true, tag, show.asInstanceOf))
    val builders = cols.map(c =>
      c.tag match
        case given ClassTag[t2] =>
          Array.newBuilder[t2]
    )
    var len = 0
    while it.hasNext do
      val row = it.next()
      len += 1
      val p = row.asInstanceOf[Product]
      var i = 0
      while i < p.productArity do
        builders(i) match
          case b: ArrayBuilder[t3] => b.addOne(p.productElement(i).asInstanceOf[t3])
        i += 1
    val data0 = builders.map[AnyRef](_.result())
    DataFrame(cols, len, data0)

  def readCSV[T](
      src: IterableOnce[String]
  )(using
      ps: ParsersOf[NamedTuple.From[T]],
      ts: TagsOf[NamedTuple.From[T]]
  ): DataFrame[T] =
    readCSVCore(
      src,
      Some(ts.names),
      [T] => ps.parsers(_).asInstanceOf[DFCSVParser[T]],
      [T] => ts.shows(_).asInstanceOf[DFShow[T]],
      ts.tags
    )

  def readCSV[T](
      path: String
  )(using
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
          case given ClassTag[t4] =>
            Array.newBuilder[t4]
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
          case given ClassTag[t5] =>
            val parser = ps[t5](i)
            val parsed = parser.parse(cell)
            dBufs(i).asInstanceOf[mutable.Builder[t5, Array[t5]]].addOne(parsed.asInstanceOf[t5])
      }

    val cBuf = IArray.newBuilder[Col[?]]
    loop(colNames)((name, i) =>
      tag(i) match
        case tag @ given ClassTag[t6] =>
          cBuf += Col(name, isDense = true, tag, show[t6](i))
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
  outerDf =>

  def columns[Exprs <: Tuple](
      f: DataFrame.Ref[T] ?=> Exprs
  )[Ls <: Tuple: {DataFrame.IsExprLabels[Exprs], SubNames[T]}]: DataFrame[FilterNames[Ls, T]] =
    val strings =
      f(using DataFrame.Ref[T]()).toIArray.map(_.asInstanceOf[DataFrame.ColRef[?, ?]].name)
    columnsRaw(strings)

  def columnsAny(names: String*): DataFrame[Any] =
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
      // pre: all names are valid
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
        case given ClassTag[t7] =>
          if col.isDense then {
            val colInit = row.asInstanceOf[Array[t7]]
            val colRest = other.data(i).asInstanceOf[Array[t7]]
            val merged = colInit ++ colRest
            dBuf += merged
          } else {
            val sparse = row.asInstanceOf[SparseArr[t7]]
            val sparseRest = other.data(i).asInstanceOf[SparseArr[t7]]
            val merged = SparseArr(
              sparse.untils ++ sparseRest.untils.map(_ + len),
              sparse.values ++ sparseRest.values
            )
            dBuf += merged
          }
    }
    DataFrame(cols, len + other.len, dBuf.result())
  }

  def withValue[F <: AnyNamedTuple: {TagsOf as ts}](f: F)(using
      Tuple.Disjoint[Names[F], Names[NamedTuple.From[T]]] =:= true
  ): DataFrame[NamedTuple.Concat[NamedTuple.From[T], F]] =
    val vs = f.asInstanceOf[Tuple].toIArray
    val cBuf = IArray.newBuilder[Col[?]] ++= cols
    val dBuf = IArray.newBuilder[AnyRef] ++= data
    val dfShows = ts.shows
    val names = ts.names
    def showOf[T](i: Int): DFShow[T] = dfShows(i).asInstanceOf[DFShow[T]]
    DataFrame.loop(ts.tags):
      case (tag @ given ClassTag[t8], i) =>
        val v: t8 = vs(i).asInstanceOf[t8]
        cBuf += Col[t8](names(i), isDense = false, tag, showOf(i))
        dBuf += SparseArr[t8](IArray(len), IArray(v))
    DataFrame(cBuf.result(), len, dBuf.result())

  def withComputed[F <: AnyNamedTuple](
      f: DataFrame.Ref[T] ?=> F
  )[
      R <: AnyNamedTuple: {IsExprRes[F], TagsOf as ts},
      C <: AnyNamedTuple: IsConcat[NamedTuple.From[T], R]
  ]: DataFrame[C] =
    val ref = new DataFrame.Ref[T]
    val exprFuncs =
      f(using ref)
        .asInstanceOf[Tuple]
        .toIArray
        .map: e =>
          val e0 = e.asInstanceOf[DataFrame.Expr[?, ?]]
          DataFrame.compile(this, e0)
    val names = ts.names
    val cBuf = IArray.newBuilder[Col[?]] ++= cols
    val dBuf = IArray.newBuilder[AnyRef] ++= data
    val dfShows = ts.shows
    def showOf[T](i: Int): DFShow[T] = dfShows(i).asInstanceOf[DFShow[T]]
    DataFrame.loop(ts.tags):
      case (tag @ given ClassTag[t9], i) =>
        val idxToValue = exprFuncs(i).asInstanceOf[ExprFunc[t9]]
        cBuf += Col[t9](names(i), isDense = true, tag, showOf[t9](i))
        dBuf += {
          val arr = new Array[t9](len)
          var j = 0
          while j < len do
            arr(j) = idxToValue(j)
            j += 1
          arr
        }
    DataFrame(cBuf.result(), len, dBuf.result())
  end withComputed

  def sort[F <: DataFrame.ColExpr](
      f: DataFrame.Ref[T] ?=> F,
      descending: Boolean = false
  )[
      Col <: String: {DataFrame.ColName[F], SubName[T]},
      V: {DataFrame.ColType[F], Ordering as ord}
  ]: DataFrame[T] =
    val name = f(using DataFrame.Ref[T]()).name
    val dataIdx = cols.indexWhere(_.name == name)
    val col = cols(dataIdx)

    // todo: use ranges if it is a sparse column
    val indices =
      col.tag match
        case given ClassTag[V @unchecked] =>
          val ord0 = if descending then ord.reverse else ord
          // Get values from the sort column
          val getElem =
            if col.isDense then
              val d = data(dataIdx).asInstanceOf[Array[V]]
              (i: java.lang.Integer) => d(i.intValue)
            else
              val sparse = data(dataIdx).asInstanceOf[SparseArr[V]]
              (i: java.lang.Integer) => sparse(i.intValue)

          // Create and sort indices based on column values
          val indices = Array.tabulate(len)(java.lang.Integer.valueOf)
          java.util.Arrays.sort(indices, ord0.on(getElem))
          indices.map(_.intValue)
    end indices

    // Remap all columns based on sorted indices
    val dBuf = IArray.newBuilder[AnyRef]
    DataFrame.loop(cols) { (col, i) =>
      col.tag match
        case given ClassTag[c] =>
          if col.isDense then
            val oldArr = data(i).asInstanceOf[Array[c]]
            val newArr = Array.ofDim[c](len)
            var j = 0
            while j < len do
              newArr(j) = oldArr(indices(j))
              j += 1
            dBuf += newArr
          else
            // todo: keep sparse?
            val sparse = data(i).asInstanceOf[SparseArr[c]]
            val newArr = Array.ofDim[c](len)
            var j = 0
            while j < len do
              newArr(j) = sparse(indices(j))
              j += 1
            dBuf += newArr
    }

    DataFrame(cols.map(_.copy(isDense = true)), len, dBuf.result())
  end sort

  def groupBy[F <: DataFrame.ColExpr](
      f: DataFrame.Ref[T] ?=> F
  )[
      Col <: String: {DataFrame.ColName[F], SubName[T]},
      K <: AnyNamedTuple: DataFrame.Key[Col, T]
  ]: DataFrame.GroupBy[K, T] =
    import scala.collection.mutable
    val ref = f(using DataFrame.Ref[T]())
    val name = ref.name
    val dataIdx = cols.indexWhere(_.name == name)
    val col = cols(dataIdx)

    def packCollsSparse[N <: String, K: ClassTag](
        buckets: mutable.HashMap[K, mutable.ArrayBuffer[Range]]
    ): DataFrame.GroupBy[Single[N, K], T] =
      val dCols =
        val cBuf = IArray.newBuilder[DataFrame.Col[?]]
        DataFrame.loop(cols) { (col, i) =>
          cBuf += (if i == dataIdx then col.copy(isDense = false) else col)
        }
        cBuf.result()
      val keysData = buckets.keySet.toArray
      val keys =
        DataFrame[Single[N, K]](
          IArray(col.copy(isDense = true)),
          keysData.length,
          IArray(keysData)
        )
      DataFrame.GroupByImpl(outerDf, Right(buckets), dataIdx, keys, dCols)
    end packCollsSparse

    def packColls[N <: String, K: ClassTag](
        buckets: mutable.HashMap[K, mutable.BitSet]
    ): DataFrame.GroupBy[Single[N, K], T] =
      val dcols =
        val cBuf = IArray.newBuilder[DataFrame.Col[?]]
        DataFrame.loop(cols) { (col, i) =>
          cBuf += (if i == dataIdx then col.copy(isDense = false)
                   else if col.isDense then col
                   else col.copy(isDense = true))
        }
        cBuf.result()
      val keysData = buckets.keySet.toArray
      val keys =
        DataFrame[Single[N, K]](
          IArray(col.copy(isDense = true)),
          keysData.length,
          IArray(keysData)
        )
      DataFrame.GroupByImpl(outerDf, Left(buckets), dataIdx, keys, dcols)
    end packColls

    type Name = Tuple.Head[Names[K]] & String
    type Target = DataFrame.GroupBy[K, T]

    col.tag match
      case given ClassTag[t0] =>
        val res: DataFrame.GroupBy[Single[Name, t0], T] =
          if col.isDense then
            val arr = data(dataIdx).asInstanceOf[IArray[t0]]
            val buckets = mutable.HashMap.empty[t0, mutable.BitSet]
            DataFrame.loop(arr)((elem, i) =>
              buckets.getOrElseUpdate(elem, mutable.BitSet.empty).add(i)
            )
            packColls[Name, t0](buckets)
          else
            val sparse = data(dataIdx).asInstanceOf[SparseArr[t0]]
            val buckets = mutable.HashMap.empty[t0, mutable.ArrayBuffer[Range]]
            DataFrame.loopRanges(sparse): (cell, from, limit) =>
              buckets.getOrElseUpdate(cell, mutable.ArrayBuffer.empty).addOne(from until limit)
            packCollsSparse[Name, t0](buckets)
          end if
        res.asInstanceOf[Target]
    end match
  end groupBy

  def debugColumns: Seq[String] =
    cols.zipWithIndex
      .map((c, i) =>
        s"[name:=${c.name}, isDense:=${c.isDense}, tag:=${c.tag}, data:=${data.lift(i)}]"
      )

  def show(n: Int = 10): String = DataFrame.showDF(this, n)

  def materialiseRows(using m: Mirror.ProductOf[T]): Iterator[T] =
    val colGetters = cols.map(c => DataFrame.compile[Any](this, DataFrame.ColRef(c.name)))
    class arrProduct(arr: IArray[Any]) extends Product:
      def productArity: Int = arr.length
      def productElement(i: Int): Any = arr(i)
      def canEqual(that: Any): Boolean = false

    Iterator.tabulate(len): i =>
      val rowData = colGetters.map(_(i))
      m.fromProduct(arrProduct(rowData))

end DataFrame
