package ntdataframe

import NamedTuple.AnyNamedTuple
import NamedTuple.Names
import NamedTuple.NamedTuple

object TupleUtils:
  trait NamesOf[T <: AnyNamedTuple]:
    def names: IArray[String]

  object NamesOf:
    final class NamesOfNT[T <: AnyNamedTuple](ns: Tuple) extends NamesOf[T]:
      val names = ns.toIArray.map(_.asInstanceOf[String])

    transparent inline given [T <: AnyNamedTuple]: NamesOf[T] =
      new NamesOfNT[T](compiletime.constValueTuple[Names[T]])

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
