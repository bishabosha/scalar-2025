package ntforeign

import scala.NamedTuple.NamedTuple
import scala.NamedTuple.AnyNamedTuple
import scala.NamedTuple.Names
import scala.NamedTuple.DropNames
import scala.NamedTuple.withNames
import java.lang.foreign.{MemoryLayout, MemorySegment, ValueLayout, SequenceLayout}
// import java.lang.foreign.{MemoryLayouts, CLinker}
import java.lang.invoke.VarHandle
import java.lang.foreign.Arena
import scala.compiletime.ops.int.S
import java.lang.foreign.StructLayout
import java.lang.foreign.MemoryLayout.PathElement

/** A type-safe wrapper over Java's Foreign Memory API to allocate struct-like types off-heap with a
  * named-tuple interface.
  */
object OffHeapStruct:

  /** Type class to map between Scala types and memory layouts
    */
  trait LayoutProvider[T]:
    def layout: ValueLayout
    def read(segment: MemorySegment, offset: Long): T
    def write(segment: MemorySegment, offset: Long, value: T): Unit

  // LayoutProvider implementations for common types
  given LayoutProvider[Int] with
    def layout: ValueLayout.OfInt = ValueLayout.JAVA_INT
    def read(segment: MemorySegment, offset: Long): Int =
      segment.get(layout, offset)
    def write(segment: MemorySegment, offset: Long, value: Int): Unit =
      segment.set(layout, offset, value)

  given LayoutProvider[Long] with
    def layout: ValueLayout.OfLong = ValueLayout.JAVA_LONG
    def read(segment: MemorySegment, offset: Long): Long =
      segment.get(layout, offset)
    def write(segment: MemorySegment, offset: Long, value: Long): Unit =
      segment.set(layout, offset, value)

  given LayoutProvider[Float] with
    def layout: ValueLayout.OfFloat = ValueLayout.JAVA_FLOAT
    def read(segment: MemorySegment, offset: Long): Float =
      segment.get(layout, offset)
    def write(segment: MemorySegment, offset: Long, value: Float): Unit =
      segment.set(layout, offset, value)

  given LayoutProvider[Double] with
    def layout: ValueLayout.OfDouble = ValueLayout.JAVA_DOUBLE
    def read(segment: MemorySegment, offset: Long): Double =
      segment.get(layout, offset)
    def write(segment: MemorySegment, offset: Long, value: Double): Unit =
      segment.set(layout, offset, value)

  given LayoutProvider[Boolean] with
    def layout: ValueLayout.OfBoolean = ValueLayout.JAVA_BOOLEAN
    def read(segment: MemorySegment, offset: Long): Boolean =
      segment.get(layout, offset)
    def write(segment: MemorySegment, offset: Long, value: Boolean): Unit =
      segment.set(layout, offset, value)

  given LayoutProvider[Byte] with
    def layout: ValueLayout.OfByte = ValueLayout.JAVA_BYTE
    def read(segment: MemorySegment, offset: Long): Byte =
      segment.get(layout, offset)
    def write(segment: MemorySegment, offset: Long, value: Byte): Unit =
      segment.set(layout, offset, value)

  // Recursive type class for handling Tuple elements
  trait TupleLayoutProvider[N <: Tuple, V <: Tuple]:
    def totalSize: Long
    // def layouts: List[MemoryLayout]
    def layoutProviders: IArray[LayoutProvider[?]]
    def offsets: IArray[Long]
    def read(segment: MemorySegment): NamedTuple[N, V]
    def write(segment: MemorySegment, values: NamedTuple[N, V]): Unit

  trait SummonAll[T <: Tuple]:
    def values: IArray[AnyRef]

  object SummonAll:
    class Impl[T <: Tuple](t: T) extends SummonAll[T]:
      val values: IArray[AnyRef] = t.toIArray
    transparent inline given [T <: Tuple]: SummonAll[T] = Impl(compiletime.summonAll[T])

  trait NamesOf[T <: Tuple]:
    def values: IArray[String]

  object NamesOf:
    class Impl[T <: Tuple](t: T) extends NamesOf[T]:
      val values: IArray[String] = t.toIArray.map(_.asInstanceOf[String])

    transparent inline given [T <: Tuple]: NamesOf[T] = Impl(compiletime.constValueTuple[T])

  // Recursive implementation for non-empty tuples
  given NT: [Ns <: Tuple: {NamesOf as ns}, Vs <: Tuple]
    => (
        all: SummonAll[Tuple.Map[Vs, LayoutProvider]]
  ) => TupleLayoutProvider[Ns, Vs]:

    val layoutProviders: IArray[LayoutProvider[?]] =
      all.values.map(_.asInstanceOf[LayoutProvider[?]])

    private val slayout: StructLayout =
      val maxAlignment = layoutProviders.map(_.layout.byteAlignment()).max
      val layouts = IArray.newBuilder[MemoryLayout]
      for (lp, n) <- layoutProviders.iterator.zip(ns.values.iterator) do
        layouts += lp.layout.withName(n)
        if lp.layout.byteAlignment() < maxAlignment then
          layouts += MemoryLayout.paddingLayout(maxAlignment - lp.layout.byteAlignment())
      MemoryLayout.structLayout(layouts.result()*)

    val offsets: IArray[Long] =
      ns.values.map(n => slayout.byteOffset(PathElement.groupElement(n)))

    // Calculate memory layouts with padding for alignment
    val totalSize: Long =
      slayout.byteSize()

    def read(segment: MemorySegment): NamedTuple[Ns, Vs] = {
      val buf = IArray.newBuilder[AnyRef]
      for (lp, offset) <- layoutProviders.iterator.zip(offsets.iterator) do
        buf += lp.read(segment, offset).asInstanceOf[AnyRef]
      Tuple.fromIArray(buf.result()).asInstanceOf[Vs].withNames[Ns]
    }

    def write(segment: MemorySegment, values: NamedTuple[Ns, Vs]): Unit = {
      val tupleIts = values.toTuple.productIterator.zip(layoutProviders.iterator)
      for case ((value, lp: LayoutProvider[t]), offset) <- tupleIts.zip(offsets.iterator) do
        lp.write(segment, offset, value.asInstanceOf[t])
    }

  /** Main API: A typed struct allocated off-heap with named tuple accessors
    */
  class Struct[T <: AnyNamedTuple] private[OffHeapStruct] (private val segment: MemorySegment)(using
      layoutProvider: TupleLayoutProvider[Names[T], DropNames[T]]
  ):
    type N = Names[T]
    type V = DropNames[T]

    // Get the current values as a named tuple
    def get: T = layoutProvider.read(segment).asInstanceOf[T]

    // Update the struct with a new named tuple
    def update(values: T): Unit =
      layoutProvider.write(segment, values.asInstanceOf[NamedTuple[Names[T], DropNames[T]]])

    // Access to the underlying memory segment
    def address: Long = segment.address()

    // Helper to access specific fields by name
    def apply[FieldName <: String & Singleton, T](fieldName: FieldName)(using
        ev: Tuple.Elem[N, IndexOf[N, FieldName]] =:= FieldName,
        idx: ValueOf[IndexOf[N, FieldName]]
    ): Tuple.Elem[V, IndexOf[N, FieldName]] =
      val i = idx.value
      val offset = layoutProvider.offsets(i)
      layoutProvider
        .layoutProviders(i)
        .asInstanceOf[LayoutProvider[T]]
        .read(segment, offset)
        .asInstanceOf[Tuple.Elem[V, IndexOf[N, FieldName]]]

    // Update a specific field
    def update[FieldName <: String & Singleton, T](fieldName: FieldName, value: T)(using
        idx: ValueOf[IndexOf[N, FieldName]],
        ev: Tuple.Elem[V, IndexOf[N, FieldName]] =:= T
    ): Unit = {
      val i = idx.value
      val offset = layoutProvider.offsets(i)
      layoutProvider
        .layoutProviders(i)
        .asInstanceOf[LayoutProvider[T]]
        .write(segment, offset, value)
    }

  // Factory methods
  def allocate[T <: AnyNamedTuple](using
      layoutProvider: TupleLayoutProvider[Names[T], DropNames[T]],
      arena: Arena
  ): Struct[T] =
    new Struct[T](arena.allocate(layoutProvider.totalSize))

  def allocateWithValues[T <: AnyNamedTuple](
      values: T
  )(using layoutProvider: TupleLayoutProvider[Names[T], DropNames[T]], arena: Arena): Struct[T] = {
    val struct = new Struct[T](arena.allocate(layoutProvider.totalSize))
    struct.update(values)
    struct
  }

  type IndexOf[N <: Tuple, F <: String] = IndexOf0[N, F, 0]

  type IndexOf0[N <: Tuple, F <: String, Acc <: Int] <: Int = N match
    case EmptyTuple  => -1
    case (F *: _)    => Acc
    case (_ *: rest) => IndexOf0[rest, F, S[Acc]]

end OffHeapStruct
