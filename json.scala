package ntjson

import upickle.default.*
import NamedTuple.AnyNamedTuple
import NamedTuple.NamedTuple
import scala.deriving.Mirror
import scala.reflect.ClassTag
import upickle.default
import upickle.core.Visitor
import upickle.core.ObjVisitor

transparent inline given autoNtWriter[T <: AnyNamedTuple: {Mirror.ProductOf as m}]: Writer[T] =
  NTObjWriter[m.MirroredElemLabels, m.MirroredElemTypes](
    fieldNames = compiletime.constValueTuple[m.MirroredElemLabels],
    fieldWriters = compiletime.summonAll[Tuple.Map[m.MirroredElemTypes, Writer]]
  ).asInstanceOf[ObjectWriter[T]]

final class NTObjWriter[N <: Tuple, V <: Tuple](fieldNames: => Tuple, fieldWriters: => Tuple)
  extends ObjectWriter[NamedTuple[N, V]] {
    private lazy val fW = fieldWriters
    private lazy val fN = fieldNames

    override def length(v: NamedTuple[N, V]): Int = fN.size

    override def writeToObject[R](ctx: ObjVisitor[?, R], v: NamedTuple[N, V]): Unit =
      val iN = fN.productIterator.asInstanceOf[Iterator[String]]
      val iW = fW.productIterator.asInstanceOf[Iterator[Writer[Any]]]
      val iV = v.toTuple.productIterator.asInstanceOf[Iterator[Any]]
      iN.zip(iW).zip(iV).foreach {
        case ((n, w), v) =>
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
    1, 2, 3
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

@main def upickleDemo =
  assert(
    upickle.default.write((name = "Jamie", age = 28)) == """{"name":"Jamie","age":28}"""
  )
