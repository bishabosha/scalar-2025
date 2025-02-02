package issub

import substructural.Sub.Substructural

class Page1[T]
class Pages1[T]
class Page2[T]
class Pages2[T]
class Ref1[T]

type IsPages[T] <: Boolean = T match
  case Page1[_]  => true
  case Pages1[_] => true

type IsRefPages[T] <: Boolean = T match
  case Ref1[i]  => IsPages[i]
  case _ => false

@main def test =
  ???

type NT = (
  site: (articles: (index: String, pages: Seq[String])),
  nav: Substructural.IsTreeOf[IsRefPages]
)
type NTMir = Substructural.NTEncoded[
  ("site", "nav"),
  (
    (
      Substructural.NTEncoded[
        "articles" *: EmptyTuple,
        (
          Substructural.NTEncoded[
            ("index", "pages"),
            (String, Seq[String])
          ]
        ) *: EmptyTuple
      ]
    ),
    (
      Substructural.NTEncoded[
        "Article" *: EmptyTuple,
        (Ref1[Pages1[(foo: Int)]]) *: EmptyTuple
      ]
    )
  )
]

// val p: other.ZipMaskEncoded[NT, NTMir, other.NTParser.MapSub] = true
val p = other.NTParser.of[NT, NTMir]
val q1 = p.site.articles.index
val q2 = p.nav.yesIAmATreeParser


// val m2: other.IsNTEncoded[NTMir] = true
//   type Page1to2[T] = T match
//     case Page1[t]  => Page2[t]
//     case Pages1[t] => Pages2[t]

val y1: Substructural[(foo: (bar: 1)), (foo: (bar: Int, qux: String), other: Boolean)] = Substructural.proven
//   val y2 =
//     Substructural.provenLeaf[(foo: (index: Page1[String], pages: Pages1[String])), Page1to2]

// val Example = (
//   foo = (bar = 1)
// )

// val y3 = Substructural.provenZoom[(foo: (bar: Substructural.Z)), Example.type]
// val y4 = Substructural.isNamedTuple[Example.type]

//   def y3 =
//     summon[y2.Out =:= (foo: (index: Page2[String], pages: Pages2[String]))]
