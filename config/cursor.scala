package confignt.cursor

import NamedTuple.AnyNamedTuple
import NamedTuple.NamedTuple
import scala.annotation.implicitNotFound

import scala.util.NotGiven

sealed trait Cursor[T] extends Selectable:
  final type Ref = T
  final type Fields = NamedTuple.Map[T & AnyNamedTuple, Cursor]
  final type AtLeaf = NotGiven[T <:< AnyNamedTuple]
  def focus(using @implicitNotFound("Cannot focus on a non-leaf node") ev: AtLeaf): T
  def selectDynamic(name: String): Cursor[?]

object Cursor:
  type IsNamedTuple[T] = T match
    case NamedTuple.NamedTuple[_, _] => true
    case _                           => None.type

  type ExtractNT[T] <: (Tuple, Tuple) = T match
    case NamedTuple.NamedTuple[ns, vs] => (ns, vs)

  inline def isNamedTupleType[T]: Boolean = inline compiletime.constValueOpt[IsNamedTuple[T]] match
    case Some(_) => true
    case _       => false

  inline def apply[T <: AnyNamedTuple](t: T): Cursor[T] =
    inline if isNamedTupleType[T] then applyInner[T](t)
    else compiletime.error("Cursor can only be created from a concrete NamedTuple")

  def compose[T](names: Tuple, values: List[Cursor[?]]): CursorImpl[T] =
    CursorImpl.Node(Map.from(names.productIterator.asInstanceOf[Iterator[String]].zip(values)))

  inline def applyInner[T](t: T): Cursor[T] =
    inline if isNamedTupleType[T] then
      inline compiletime.erasedValue[ExtractNT[T]] match
        case _: (ns, vs) =>
          def createNode: CursorImpl[T] =
            val keys = compiletime.constValueTuple[ns]
            val values = mapInner[vs, vs](t.asInstanceOf[vs], 0)
            compose[T](keys, values)
          createNode
    else CursorImpl.Leaf(t)

  inline def mapInner[Ts <: Tuple, Sub <: Tuple](ts: Ts, idx: Int): List[Cursor[?]] =
    inline compiletime.erasedValue[Sub] match
      case _: (s *: ss) =>
        applyInner[s](ts.productElement(idx).asInstanceOf[s]) :: mapInner[Ts, ss](ts, idx + 1)
      case _: EmptyTuple => Nil

enum CursorImpl[T] extends Cursor[T]:
  case Leaf(value: T)
  case Node(inner: Map[String, Cursor[?]])

  def focus(using ev: AtLeaf): T = this.asInstanceOf[Leaf[T]].value
  def selectDynamic(name: String): Cursor[?] = this.asInstanceOf[Node[T]].inner(name)
