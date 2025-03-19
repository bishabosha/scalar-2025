package ntconvert

import scala.deriving.Mirror
import NamedTuple.Names
import NamedTuple.DropNames
import NamedTuple.AnyNamedTuple
import NamedTuple.NamedTuple

sealed trait AsNamedTuple[T]:
  type Names <: Tuple
  type DropNames <: Tuple
  final type Repr = NamedTuple[Names, DropNames]
  extension (t: T)
    def narrow: Repr
    final def toTuple: DropNames = NamedTuple.toTuple(narrow)

object AsNamedTuple:
  object Impl extends AsNamedTuple[AnyNamedTuple]:
    extension (t: AnyNamedTuple) override def narrow: Repr = t.asInstanceOf[Repr]
  transparent inline given [N <: Tuple, V <: Tuple, R <: NamedTuple[N, V]]: AsNamedTuple[R] =
    Impl.asInstanceOf[AsNamedTuple[R] { type Names = N; type DropNames = V }]

extension [T <: Product, U <: AnyNamedTuple](t: T)(using
    U <:< NamedTuple.From[T]
)
  def asNamedTuple(using m: Mirror.ProductOf[U]): U =
    m.fromProduct(t)

extension [T <: AnyNamedTuple: {AsNamedTuple as reprT}](t: T)
  inline def withField[U <: AnyNamedTuple: {AsNamedTuple as reprU}](u: U)(using
      Tuple.Disjoint[reprT.Names, reprU.Names] =:= true
  ): NamedTuple.Concat[reprT.Repr, reprU.Repr] =
    t.narrow ++ u.narrow

  def as[U: {Mirror.ProductOf as m}](using T <:< NamedTuple.From[U]): U =
    m.fromProduct(t.toTuple)

case class UserV1(name: String)
case class UserV2(name: String, age: Option[Int])

def convert(u1: UserV1): UserV2 =
  u1.asNamedTuple
    .withField((age = None))
    .as[UserV2]
