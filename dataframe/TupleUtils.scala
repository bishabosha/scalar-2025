package ntdataframe

import NamedTuple.AnyNamedTuple
import NamedTuple.Names
import NamedTuple.DropNames
import NamedTuple.NamedTuple
import scala.deriving.Mirror

object TupleUtils:
  given [N <: Tuple, V <: Tuple]
    => (mn: Mirror.ProductOf[N], mv: Mirror.ProductOf[V])
    => (Mirror.ProductOf[NamedTuple[N, V]] {
      type MirroredLabel = "NamedTuple"
      type MirroredElemLabels = N
      type MirroredElemTypes = V
    }) =
    mv.asInstanceOf[
      Mirror.ProductOf[NamedTuple[N, V]] {
        type MirroredLabel = "NamedTuple"
        type MirroredElemLabels = N
        type MirroredElemTypes = V
      }
    ]

  opaque type Summoned[T <: Tuple] <: Any = T

  object Summoned:
    transparent inline given [T <: Tuple]: Summoned[T] = compiletime.summonAll[T]

    extension [T <: Tuple](t: Summoned[T])
      def head: Tuple.Head[T] = t(0)
      def toIArray: IArray[AnyRef] = t.toIArray

  trait NamesOf[T]:
    def names: IArray[String]

  object NamesOf:
    final class NamesOfNT[T](ns: Tuple) extends NamesOf[T]:
      val names = ns.toIArray.map(_.asInstanceOf[String])

    transparent inline given [T]: NamesOf[T] =
      new NamesOfNT[T](compiletime.constValueTuple[Names[NamedTuple.From[T]]])

  trait Strings[T]:
    def strings: IArray[String]

  object Strings:
    final class StringsT[T](t: Tuple) extends Strings[T]:
      val strings = t.toIArray.map(_.asInstanceOf[String])

    transparent inline given [T <: Tuple](using UpperBounded[T, String] =:= true): Strings[T] =
      new StringsT[T](compiletime.constValueTuple[T])

    type UpperBounded[T <: Tuple, N] <: Boolean = T match
      case (N *: rest) => UpperBounded[rest, N]
      case EmptyTuple  => true
      case _           => false

  type ContainsAll[X <: Tuple, Y <: Tuple] <: Boolean = X match
    case x *: xs =>
      Tuple.Contains[Y, x] match
        case true  => ContainsAll[xs, Y]
        case false => false
    case EmptyTuple => true

  type SubLabels[T] = [From <: AnyNamedTuple] =>> SubNames[T][Names[From]]

  type SubNames[T] = [N <: Tuple] =>> ContainsAll[
    N,
    Names[NamedTuple.From[T]]
  ] =:= true

  type SubName[T] = [Name <: String] =>> Tuple.Contains[
    Names[NamedTuple.From[T]],
    Name
  ] =:= true

  type InverseMapNT[T <: AnyNamedTuple, F[_]] =
    NamedTuple.Map[T, [X] =>> X match { case F[t] => t }]

  type FilterNames[N <: Tuple, T] <: AnyNamedTuple = NamedTuple.From[T] match
    case NamedTuple[ns, vs] => FilterNames0[N, ns, vs, EmptyTuple, EmptyTuple]

  type FilterName[N <: String, T] = FilterNames[N *: EmptyTuple, T]

  type Project[T <: AnyNamedTuple, F[_, _]] =
    NamedTuple[
      Names[T],
      Tuple.Map[Tuple.Zip[Names[T], DropNames[T]], [X] =>> X match { case (n, t) => F[n, t] }]
    ]

  type LookupName[N, T] = NamedTuple.From[T] match
    case NamedTuple[ns, vs] =>
      FilterName0[N, ns, vs] match
        case Some[v] => v
        case _       => Nothing

  type FilterNames0[
      Ns <: Tuple,
      Ns1 <: Tuple,
      Vs1 <: Tuple,
      AccN <: Tuple,
      AccV <: Tuple
  ] <: AnyNamedTuple =
    Ns match
      case n *: ns =>
        FilterName0[n, Ns1, Vs1] match
          case Some[v] => FilterNames0[ns, Ns1, Vs1, n *: AccN, v *: AccV]
      case EmptyTuple => NamedTuple[Tuple.Reverse[AccN], Tuple.Reverse[AccV]]

  type FilterName0[N, Ns1 <: Tuple, Vs1 <: Tuple] <: Option[Any] =
    (Ns1, Vs1) match
      case (N *: ns, v *: vs) => Some[v]
      case (_ *: ns, _ *: vs) => FilterName0[N, ns, vs]
