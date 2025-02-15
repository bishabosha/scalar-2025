package cursor

import NamedTuple.AnyNamedTuple
import NamedTuple.NamedTuple

import substructural.Sub.Substructural.WrapSub

trait Cursor extends Selectable:
  type Fields <: AnyNamedTuple
  def selectDynamic(name: String): Any

object Cursor:
  def apply[T <: AnyNamedTuple](fields: T)(using
      map: WrapSub[T, [F] =>> Cursor { type Fields = F }]
  ): Cursor { type Fields = map.Out } = ???

object Demo:
  val conf = (
    foo = (
      bar = 1,
      nested = (
        qux = "hello"
      )
    ),
    baz = true
  )

  val cursor = Cursor(conf)
  cursor.foo.nested.qux

// object Cursor:
// type Lookup[T <: AnyNamedTuple, K <: String] = T match
//   case NamedTuple[n, v] => TypeOfElem[n, v, K]

// type TypeOfElem[N <: Tuple, V <: Tuple, K <: String] = N match
//   case (N *: _) =>
//     V match
//       case (k *: _) => k
//   case (_ *: n) =>
//     V match
//       case (_ *: v) => TypeOfElem[n, v, K]
