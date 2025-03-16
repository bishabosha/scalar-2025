package ntconvert

import scala.deriving.Mirror
import NamedTuple.Names
import NamedTuple.DropNames
import NamedTuple.AnyNamedTuple
import NamedTuple.NamedTuple

extension [T](t: T)
  def asNamedTuple[U <: AnyNamedTuple](using
      U <:< NamedTuple.From[T]
  )(using m: Mirror.ProductOf[U]): U =
    m.fromProduct(t.asInstanceOf[Product])

extension [T <: AnyNamedTuple](t: T)
  inline def narrow: NamedTuple[Names[T], DropNames[T]] =
    t.asInstanceOf[NamedTuple[Names[T], DropNames[T]]]
  inline def withField[U <: AnyNamedTuple](u: U)(using
      Tuple.Disjoint[Names[T], Names[U]] =:= true
  ): NamedTuple.Concat[T, U] =
    t.narrow ++ u.narrow

  def as[U: {Mirror.ProductOf as m}](using T <:< NamedTuple.From[U]): U =
    m.fromProduct(t.asInstanceOf[Product])

case class UserV1(name: String)
case class UserV2(name: String, age: Option[Int])

def convert(u1: UserV1): UserV2 =
  u1.asNamedTuple
    .withField((age = None))
    .as[UserV2]
