package issub

import substructural.Sub.Substructural

class Page1[T]
class Pages1[T]
class Page2[T]
class Pages2[T]

@main def test =
  ???
//   type Page1to2[T] = T match
//     case Page1[t]  => Page2[t]
//     case Pages1[t] => Pages2[t]

// val y1: Substructural[(foo: (bar: 1)), (foo: (bar: Int, qux: String), other: Boolean)] = Substructural.proven
//   val y2 =
//     Substructural.provenLeaf[(foo: (index: Page1[String], pages: Pages1[String])), Page1to2]

// val Example = (
//   foo = (bar = 1)
// )

// val y3 = Substructural.provenZoom[(foo: (bar: Substructural.Z)), Example.type]
// val y4 = Substructural.isNamedTuple[Example.type]

//   def y3 =
//     summon[y2.Out =:= (foo: (index: Page2[String], pages: Pages2[String]))]
