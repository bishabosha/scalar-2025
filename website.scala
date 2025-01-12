package staticsitegen

import scala.NamedTuple.AnyNamedTuple
import scala.NamedTuple.*

// layout is a function takes a doc page (with a typed front-matter), and a site context,
//   and returns a concrete html page.

// Site context reifies the site's information structure.
//   That structure allows to access any page. From any page we can access the pages typed metadata.

// (for now) each collection should have metadata of the same type (e.g. Seq[Foo]),
//   but each collection can declare a different type.

type Article = (
    title: String,
    description: String,
    published: String
)

type Articles = (
  description: String,
)

type About = (
  description: String,
)

class Layout[T]
class Docs[Ts]
class Doc[T]
// class Refs[T <: AnyNamedTuple, U <: AnyNamedTuple](factory: Ref[T] ?=> U)

class Ref[T](names: List[String]) extends Selectable:
  // might need to capture path at the type level, so to be sure the reference corresponds to a path with correct depth
  type Fields = NamedTuple.Map[T & AnyNamedTuple, [X] =>> Ref[X]]
  def selectDynamic(name: String): Any =
    Ref(names :+ name) // create a new Ref that captures the path

def article: Layout[Article] = ???
def articles: Layout[Articles] = ???
def about: Layout[About] = ???

def mkref[T <: AnyNamedTuple](t: T): Ref[T] = Ref(Nil)
def ref[T <: AnyNamedTuple: Ref as ref]: Ref[T] = ref
// def refs[T <: AnyNamedTuple](using ref: Ref[T])[U <: AnyNamedTuple](
//     f: Ref[T] ?=> U
// ): Refs[T, U] = Refs(f)

val NoExtra: NamedTuple.Empty = NamedTuple.Empty

val Breeze = (
  metadata = (
    name = "Breeze",
    author = "github.com/bishabosha"
  ),
  layouts = (
    article = article,
    articles = articles
  ),
  site = (
    // Doc is a file of that name, and Docs is a directory of files.
    articles = (index = Doc[Articles], pages = Docs[Article]),
  ),
  extraHead = Seq.empty,
  extraFoot = Seq.empty
)
  .and:
    (
      // nav "plugin" would need to verify that each entry is a ref to a Doc[?]
      nav = (Articles = ref.site.articles.index),
    )

val Breeze2 = (
  metadata = (
    name = "Breeze2",
    author = Breeze.metadata.author
  ),
  layouts = Breeze.layouts ++ (
    about = about
  ),
  site = Breeze.site ++ (
    about = (index = Doc[About])
  ),
  extraHead = Breeze.extraHead ++ Seq.empty /* include all the goodies */,
  extraFoot = Breeze.extraFoot ++ Seq.empty /* include all the goodies */
).and:
  (
    // important to note here that currently the refs in Breeze.nav have to be resolved dynamically.
    // so context creation will have to be dynamic, e.g. return Option[Ctx].
    // to get past this, we could make nav be wrapped in some "Refs" type that captures in the type all paths referenced,
    // and then to combine two refs - the paths have to exist in the contextual Ref[T] type.
    // this seems quite verbose so not ideal.
    // e.g.
    // ```
    // Refs[(("site", "articles", "index"), ("site", "about", "index")), ???]
    // ```
    // and the syntax to construct this i dont know yet.
    nav = (About = ref.site.about.index) ++ Breeze.nav,
  )

extension [T <: AnyNamedTuple](t: T)
  def and[U <: AnyNamedTuple](f: Ref[T] ?=> U)(using
      ev: Tuple.Disjoint[Names[T], Names[U]] =:= true
  ): NamedTuple.Concat[T, U] =
    val t1: NamedTuple[Names[T], DropNames[T]] = t.asInstanceOf
    val u: NamedTuple[Names[U], DropNames[U]] = f(using mkref(t)).asInstanceOf
    t1 ++ u

// object Breeze extends model.Theme:

//   val metadata = new {
//     val name = "Breeze"
//     val layouts = Layouts(
//       article = breeze.articleLayout,
//       articles = breeze.articles
//     )
//   }

//   type Site = model.Site {
//     val about: Doc
//     val articles: Docs
//   }

//   type FrontMatter = model.FrontMatter {
//     val title: String
//     val published: String
//     val startDate: String
//     val avatar: String
//     val links: List[String]
//     val name: String
//     val copyright: String
//     val subtitle: String
//     val url: String
//     val description: String
//     val isIndexOnly: Boolean
//     val isInProgress: Boolean
//     val ordered: String // a helper to order items
//   }

//   trait Extra(using Context):
//     def nav: List[DocCollection] = List(ctx.site.about, ctx.site.articles)
//     val extraHead: Seq[scalatags.Text.all.Modifier]
//     val extraFoot: Seq[scalatags.Text.all.Modifier]

//   def extras(using Context, model.Context.InMakeCtx): Extra = new {
//     val extraHead = Seq.empty
//     val extraFoot = Seq.empty
//   }

//   def whoAmI(using Context): String = ctx.site.about.page.frontMatter.name
//   def copyright(using Context): String =
//     ctx.site.about.page.frontMatter.copyright

// sealed trait Layouts extends Selectable:
//   outer =>
//   type Fields <: AnyNamedTuple

//   def selectDynamic(name: String): Any

//   final def apply[C <: model.Context, D <: DocPage, DC <: DocCollection[D]](
//       name: String
//   )(doc: D)(using
//       C,
//       DC
//   ): ConcreteHtmlTag[String] =
//     val layout =
//       try selectDynamic(name).asInstanceOf[Layout[C, D]]
//       catch
//         case err =>
//           throw new Exception(
//             s"Layout not found: `$name` for doc ${summon[DC].collName}.${doc.name}"
//           )
//     layout(doc)

//   def ++(additions: Layouts)(using
//       Layouts.Disjoint[Fields, additions.Fields] =:= true
//   ): Layouts {
//     type Fields = NamedTuple.Concat[outer.Fields, additions.Fields]
//   } = {
//     val self = this.asInstanceOf[Reified[Names[Fields], DropNames[Fields]]]
//     val other = additions.asInstanceOf[Reified[Names[
//       additions.Fields
//     ], DropNames[additions.Fields]]]
//     Reified(self.record ++ other.record).asInstanceOf[
//       Layouts {
//         type Fields = NamedTuple.Concat[Fields, additions.Fields]
//       }
//     ]
//   }

// object Layouts:
//   type Disjoint[X <: AnyNamedTuple, Y <: AnyNamedTuple] =
//     Tuple.Disjoint[Names[X], Names[Y]]

//   type Of[Ns <: Tuple, Vs <: Tuple] = Layouts {
//     type Fields = NamedTuple[Ns, Vs]
//   }

//   class Reified[Ns <: Tuple, Vs <: Tuple](record: Map[String, Any])
//       extends Layouts:
//     type Fields = NamedTuple[Ns, Vs]
//     def selectDynamic(name: String): Any =
//       record(name)

//   inline def apply[Ns <: Tuple, Vs <: Tuple](
//       ls: NamedTuple[Ns, Vs]
//   ): Layouts.Of[Ns, Vs] =
//     Reified(utils.reify(ls))
