package issub

import substructural.Sub.Substructural
import other.IsNTEncoded
import cursor.Cursor
import staticsitegen.Ref

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
// val p = other.NTParser.of[NT, NTMir]
// val q1 = p.site.articles.index
// val q2 = p.nav.yesIAmATreeParser

def test2 =
  summon[(x: Int) =:= (x: Int)]
  summon[(x: Int) <:< (x: Int)]
  // summon[(x: Int) <:< (x: Int, y: Int)]
  summon[Substructural[(x: (y: Int)), (x: (y: Int), y: Int)]]
  summon[IsNTEncoded[NTMir] =:= true]
  summon[IsNTEncoded[Int] =:= false]

import NamedTuple.AnyNamedTuple

def SiteParser[Ctx <: AnyNamedTuple](c: other.Cursor[Ctx])(using ev: Substructural.HasRequirements[other.SiteThemeProvider.ThemeInMask, Ctx]) =
  val pBreeze = other.NTParser[Ctx, other.SiteThemeProvider.ThemeInMask, ev.Encoded]()
  val qBreeze1 = pBreeze.site.yesIAmATreeParser // TODO: tree is not practical - can have a 1 level depth dictionary of submask
  val qBreeze2 = pBreeze.nav.yesIAmATreeParser
  val qBreeze3a = pBreeze.extras.extraHead.focus
  val qBreeze3b = pBreeze.extras.extraFoot.focus
  val qBreeze4a = pBreeze.metadata.author.focus
  val qBreeze4b = pBreeze.metadata.name.focus
  // also answer the question of recursion to allow the lookup of everything.

// val qFoo = pBreeze.foo("nav").yesIAmATreeParser
// val qFoo: pBreeze.Fields = (metadata = pBreeze.metadata, site = pBreeze.site, extras = pBreeze.extras, nav = pBreeze.nav)

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

// def rec[T <: AnyNamedTuple, U <: AnyNamedTuple](f: (rec: Ref[U]) => T)(using util.NotGiven[T =:= AnyNamedTuple])(using T =:= U): T = f(???)
// val m = rec { (ref: Ref[(foo: (bar: Int))]) =>
//   (
//     foo = (bar = 1),
//     bar = ref.foo.bar
//   )
// }