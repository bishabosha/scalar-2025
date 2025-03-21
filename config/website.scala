package staticsitegen

import scala.NamedTuple.AnyNamedTuple
import scala.NamedTuple.*
import scala.Tuple.Disjoint
import javax.xml.validation.Schema

import substructural.Sub.Substructural
import scala.util.NotGiven
import scala.annotation.implicitNotFound

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
  name: String,
  description: String,
)

class Layout[T]
class Docs[Ts]
class Doc[T]

class Ref[T](names: List[String]) extends Selectable:
  // might need to capture path at the type level, so to be sure the reference corresponds to a path with correct depth
  type Fields = NamedTuple.Map[NamedTuple.From[T], [X] =>> Ref[X]]
  def selectDynamic(name: String): Any =
    Ref(names :+ name) // create a new Ref that captures the path

def mkref[T <: AnyNamedTuple](t: T): Ref[T] = Ref(Nil)
def ref[T <: AnyNamedTuple: Ref as ref]: Ref[T] = ref

val Breeze = (
  metadata = (
    name = "Breeze",
    author = "github.com/bishabosha"
  ),
  layouts = (
    article = Layout[Article],
    articles = Layout[Articles]
  ),
  site = (
    // Doc is a file of that name, and Docs is a directory of files.
    articles = (index = Doc[Articles], pages = Docs[Article]),
  ),
  extras = (
    extraHead = Seq.empty,
    extraFoot = Seq.empty
  )
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
    about = Layout[About]
  ),
  site = Breeze.site ++ (
    about = (index = Doc[About])
  ),
  extras = (
    extraHead = Breeze.extras.extraHead ++ Seq.empty,
    extraFoot = Breeze.extras.extraFoot ++ Seq.empty
  )
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
      ev: Substructural.Compose[T, U]
  ): ev.Out = t ++ f(using mkref(t))


object ConfigDSLv1:
  class Context[Ctx <: AnyNamedTuple] extends Selectable:
    type Fields = Ctx
    type Context = this.type
    def selectDynamic(name: String): Any = ???

  sealed trait Here

  object ZoomOps:
    type Zoom[P <: AnyNamedTuple, T <: AnyNamedTuple] =
      T match
        case NamedTuple[ns1, vs1] =>
          ZoomHelper[P, ns1, vs1]
        case _ => Nothing

    type ZoomHelper[P <: AnyNamedTuple, Ns1, Vs1] = P match
      case NamedTuple[ns0, vs0] => ZoomHelper0[ns0, vs0, Ns1, Vs1]
      case _                    => Nothing

    type ZoomHelper0[
        Ns0 <: Tuple,
        Ts0 <: Tuple,
        Ns1 <: Tuple,
        Ts1 <: Tuple
    ] =
      (Ns0, Ts0) match
        case (n *: _, Here *: _) => ZoomHelper1[n, Ns1, Ts1]
        case _                   => Nothing

    type ZoomHelper1[N, Ns1 <: Tuple, Ts1 <: Tuple] =
      (Ns1, Ts1) match
        case (N *: _, v *: _)     => v
        case (_ *: ns2, _ *: vs2) => ZoomHelper1[N, ns2, vs2]
        case _                    => Nothing


  // class Cursor:
  //   type Path <: AnyNamedTuple

  // object Cursor:
  //   def apply[T <: AnyNamedTuple]: Cursor { type Path = T } = new Cursor {
  //     type Path = T
  //   }

end ConfigDSLv1

object ConfigDSLv2:
  import ConfigDSLv1.*

  class Layout2[T] // currently no way in this design to consume a Layout2 that has the full site context.

  class ContextBuilder[In <: AnyNamedTuple, Out <: AnyNamedTuple](
      val src: In
  ):
    def build: Context[Out] = ???

  object ContextBuilder:
    extension [In <: AnyNamedTuple, Out <: AnyNamedTuple](
        b: ContextBuilder[In, Out]
    )
      def plugin[P <: Plugin](
          p: P
      )(dep: p.Reqs[In])(using p.CanMix[In, Out]): ContextBuilder[In, p.Mix[In, Out]] =
        p.map(b)(dep)

  def builder[In <: AnyNamedTuple](
      src0: In
  ): ContextBuilder[NamedTuple.From[In], NamedTuple.Empty] =
    new ContextBuilder(src0.asInstanceOf)

  trait Plugin:
    type Path0 <: AnyNamedTuple
    type Reqs[I <: AnyNamedTuple]
    type Extras[I <: AnyNamedTuple] <: AnyNamedTuple
    final type Z[I <: AnyNamedTuple] = ZoomOps.Zoom[Path0, I]
    final type CanMix[I <: AnyNamedTuple, O <: AnyNamedTuple] =
      Disjoint[Names[O], Names[Extras[I]]] =:= true
    final type Mix[I <: AnyNamedTuple, O <: AnyNamedTuple] =
      Concat[O, Extras[I]]
    final type ZMap[I <: AnyNamedTuple, F[T]] = Z[I] match
      case NamedTuple[ns, vs] =>
        NamedTuple[ns, Tuple.Map[
          vs,
          [X] =>> F[X]
        ]]
    def map[I <: AnyNamedTuple, O <: AnyNamedTuple](
        b: ContextBuilder[I, O]
    )(
        dep: Reqs[I]
    )(using CanMix[I, O]): ContextBuilder[I, Mix[I, O]]

  object lookupLayouts extends Plugin:
    type Path0 = (layouts: Here)
    type Extras[I <: AnyNamedTuple] = (layouts: Reqs[I])
    def map[I <: AnyNamedTuple, O <: AnyNamedTuple](
        b: ContextBuilder[I, O]
    )(dep: Reqs[I])(using CanMix[I, O]): ContextBuilder[I, Mix[I, O]] = ???

    type Reqs[I <: AnyNamedTuple] = ZMap[I, [X] =>> X match
      case Layout[a] => Layout2[a]]

  val builder0 = builder(Breeze2)

  val SiteCtx = builder0
    .plugin(lookupLayouts)(
      (
        article = Layout2[Article],
        articles = Layout2[Articles],
        about = Layout2[About]
      )
    )
    .build

end ConfigDSLv2

object ConfigDSLv3:
  import ConfigDSLv1.*

  class ContextBuilder[In <: AnyNamedTuple](
      val src: In
  ):
    type Reqs[Schema <: AnyNamedTuple] <: AnyNamedTuple
    type Output <: AnyNamedTuple
    final type Schema = NamedTuple.From[Output]

    def plugin(
        p: lookupLayouts.type
    )(using p.CanMix[Output]): ContextBuilder.Aux[In, p.MixReq[In, Reqs], p.MixOut[In, Output]] =
      p.map(this)

    def build(reqs: Reqs[Output]): Context[Output] = ???

  object ContextBuilder:
    type Aux[In <: AnyNamedTuple, Reqs0[Schema0 <: AnyNamedTuple] <: AnyNamedTuple, Out0 <: AnyNamedTuple] =
      ContextBuilder[In] {
        type Reqs[Schema <: AnyNamedTuple] = Reqs0[Schema]
        type Output = Out0
      }

  def builder[In <: AnyNamedTuple](
      src0: In
  ): ContextBuilder.Aux[NamedTuple.From[In], [X] =>> NamedTuple.Empty, NamedTuple.Empty] =
    new ContextBuilder(src0.asInstanceOf).asInstanceOf

  trait Plugin:
    final type IsDisjoint[A <: AnyNamedTuple, B <: AnyNamedTuple] =
      Disjoint[Names[A], Names[B]] =:= true

    final type CanMix[O <: AnyNamedTuple] =
      IsDisjoint[O, ExtraOutKeys[Here]]

    final type Z[I <: AnyNamedTuple] = ZoomOps.Zoom[ExtraInKeys[Here], I]
    final type MixOut[I <: AnyNamedTuple, O <: AnyNamedTuple] =
      Concat[O, ExtraOutKeys[MkOut[I]]]
    final type MixReq[I <: AnyNamedTuple, F[Schema0 <: AnyNamedTuple] <: AnyNamedTuple] = [Schema <: AnyNamedTuple] =>>
      Concat[F[Schema], ExtraInKeys[MkReq[I, Schema]]]
    final type ZMap[I <: AnyNamedTuple, F[T]] <: AnyNamedTuple = Z[I] match
      case NamedTuple[ns, vs] =>
        NamedTuple[ns, Tuple.Map[
          vs,
          [X] =>> F[X]
        ]]
      case _ => Nothing

    type MkOut[I <: AnyNamedTuple] <: AnyNamedTuple
    type MkReq[I <: AnyNamedTuple, Schema <: AnyNamedTuple] <: AnyNamedTuple
    type ExtraOutKeys[Out] <: AnyNamedTuple
    type ExtraInKeys[Out] <: AnyNamedTuple

    def map[I <: AnyNamedTuple](
        b: ContextBuilder[I]
    )(using CanMix[b.Output]): ContextBuilder.Aux[I, MixReq[I, b.Reqs], MixOut[I, b.Output]]
  end Plugin

  object lookupLayouts extends Plugin:
    type ExtraInKeys[Out] = (layouts: Out)
    type ExtraOutKeys[Out] = ExtraInKeys[Out]

    override type MkOut[I <: AnyNamedTuple] = ZMap[I, [X] =>> X match
      case Layout[a] => Layout2[a]
    ]
    override type MkReq[I <: AnyNamedTuple, Schema <: AnyNamedTuple] = ZMap[I, [X] =>> X match
      case Layout[a] => Layout3[a, Schema]
    ]
    def map[I <: AnyNamedTuple](
        b: ContextBuilder[I]
    )(using CanMix[b.Output]): ContextBuilder.Aux[I, MixReq[I, b.Reqs], MixOut[I, b.Output]] = ???

  object navPlugin extends Plugin:
    type ExtraInKeys[Out] = (nav: Out)
    type ExtraOutKeys[Out] = (nav: Out)

    override type MkOut[I <: AnyNamedTuple] = NamedTuple.Empty
    override type MkReq[I <: AnyNamedTuple, Schema <: AnyNamedTuple] = ZMap[I, [X] =>> X match
      case Layout[a] => Layout3[a, Schema]
    ]
    def map[I <: AnyNamedTuple](
        b: ContextBuilder[I]
    )(using CanMix[b.Output]): ContextBuilder.Aux[I, MixReq[I, b.Reqs], MixOut[I, b.Output]] = ???

  val SiteBuilder = builder(Breeze)
    .plugin(lookupLayouts)

  val SiteCtx = SiteBuilder
    .build(
      (
        layouts = (
          article = article2,
          articles = articles2,
        )
      )
    )

  val SiteBuilder2 = builder(Breeze2)
    .plugin(lookupLayouts)

  val SiteCtx2 = SiteBuilder2
    .build(
      (
        layouts = (
          article = Layout3(article2).widen,
          articles = Layout3(articles2).widen,
          about = about2
        )
      )
    )

  class Page[T] extends Selectable:
    type Fields = NamedTuple.From[T]
    def selectDynamic(name: String): Any = ???

  class Layout2[T]

  trait Layout3[T, Schema <: AnyNamedTuple] extends Layout2[T]:
    outer =>
    def render(page: Page[T], ctx: Context[Schema]): String

    def widen[Schema2 <: AnyNamedTuple](using Substructural[Schema, Schema2]): Layout3[T, Schema2] =
      new {
        def render(page: Page[T], ctx: Context[Schema2]): String = outer.render(page, ctx.asInstanceOf)
      }.asInstanceOf

  object Layout3:
    def apply[T, Schema <: AnyNamedTuple](
        f: (Page[T], Context[Schema]) => String
    ): Layout3[T, NamedTuple.From[Schema]] =
      new {
        def render(page: Page[T], ctx: Context[Schema]): String = f(page, ctx)
      }.asInstanceOf

  def article2(page: Page[Article], ctx: Context[SiteBuilder.Schema]): String =
    s"""
    <h1>${page.title}</h1>
    <p>${page.description}</p>
    <p>${page.published}</p>
    <ul>${ctx.layouts.article}</ul>
    """

  def articles2(page: Page[Articles], ctx: Context[SiteBuilder.Schema]) =
    s"""
    <h1>Articles</h1>
    <p>${page.description}</p>
    ${for article <- 1 to 10 yield "??? (TODO: lookup article in ctx)"}
    <ul>${ctx.layouts.article}</ul>
    """

  def about2(page: Page[About], ctx: Context[SiteBuilder2.Schema]) =
    s"""
    <h1>About ${page.name}</h1>
    <p>${page.description}</p>
    """

end ConfigDSLv3