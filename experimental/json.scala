package ntjson

import upickle.default.*
import NamedTuple.AnyNamedTuple
import NamedTuple.NamedTuple
import NamedTuple.withNames
import scala.deriving.Mirror
import scala.reflect.ClassTag
import upickle.default
import upickle.core.Visitor
import upickle.core.ObjVisitor

inline given autoNtWriter[T <: AnyNamedTuple: {Mirror.ProductOf as m}]: Writer[T] =
  NTObjWriter[m.MirroredElemLabels, m.MirroredElemTypes](
    fieldNames = compiletime.constValueTuple[m.MirroredElemLabels],
    fieldWriters = compiletime.summonAll[Tuple.Map[m.MirroredElemTypes, Writer]]
  ).asInstanceOf[ObjectWriter[T]]

inline given autoNtReader[T <: AnyNamedTuple: {Mirror.ProductOf as m}]: Reader[T] =
  NTObjReader[m.MirroredElemLabels, m.MirroredElemTypes](
    paramCount = compiletime.constValue[NamedTuple.Size[m.MirroredType]],
    fieldNames = compiletime.constValueTuple[m.MirroredElemLabels],
    fieldReaders = compiletime.summonAll[Tuple.Map[m.MirroredElemTypes, Reader]]
  ).asInstanceOf[Reader[T]]

final class NTObjWriter[N <: Tuple, V <: Tuple](fieldNames: => Tuple, fieldWriters: => Tuple)
    extends ObjectWriter[NamedTuple[N, V]] {
  private lazy val fW = fieldWriters
  private lazy val fN = fieldNames

  override def length(v: NamedTuple[N, V]): Int = fN.size

  override def writeToObject[R](ctx: ObjVisitor[?, R], v: NamedTuple[N, V]): Unit =
    val iN = fN.productIterator.asInstanceOf[Iterator[String]]
    val iW = fW.productIterator.asInstanceOf[Iterator[Writer[Any]]]
    val iV = v.toTuple.productIterator.asInstanceOf[Iterator[Any]]
    iN.zip(iW).zip(iV).foreach { case ((n, w), v) =>
      val keyVisitor = ctx.visitKey(-1)
      ctx.visitKeyValue(
        keyVisitor.visitString(n, -1)
      )
      ctx.narrow.visitValue(w.write(ctx.subVisitor, v), -1)
    }

  override def write0[V1](out: Visitor[?, V1], v: NamedTuple[N, V]): V1 =
    val oVisitor = out.visitObject(fN.size, jsonableKeys = true, -1)
    writeToObject(oVisitor, v)
    oVisitor.visitEnd(-1)
}

final class NTObjReader[N <: Tuple, V <: Tuple](
    paramCount: Int,
    fieldNames: => Tuple,
    fieldReaders: => Tuple
) extends CaseClassReader3V2[NamedTuple[N, V]](
      paramCount,
      if (paramCount <= 64) if (paramCount == 64) -1 else (1L << paramCount) - 1 else paramCount,
      allowUnknownKeys = false,
      (params, _) => Tuple.fromArray(params).asInstanceOf[V].withNames[N]
    ) {
  lazy val fR = fieldReaders.toArray
  lazy val fN = fieldNames.toArray.map(_.asInstanceOf[String])
  override def visitors0 = (null, fR)
  override def keyToIndex(x: String): Int = fN.indexOf(x)
  override def allKeysArray = fN
  override def storeDefaults(x: upickle.implicits.BaseCaseObjectContext): Unit = ()
}

// pair with some nt query type for the reader
// port over the scala center full-stack-app to ops-mirror server?
// and use the nt query lib for the repository store.
// potentially the config parser for some static paths?

def mkFoo(baz: String) = (
  foo = 23,
  bar = (
    baz = baz,
    qux = 43
  ),
  other = true,
  arr = (
    1,
    2,
    3
  )
)

@main def demo =
  println(
    write(
      Seq(
        mkFoo("qux"),
        mkFoo("quux")
      )
    )
  )

@main def demoRead =
  val json = read[(name: String, age: Int)]("""{"name":"Jamie","age":28}""")
  println(s"Name: ${json.name}, Age: ${json.age}")

@main def upickleDemo =
  assert(
    upickle.default.write((name = "Jamie", age = 28)) == """{"name":"Jamie","age":28}"""
  )
