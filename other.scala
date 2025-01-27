package other

// import staticsitegen.NuConfig.BreezeTheme
import substructural.Sub.Substructural
import scala.util.NotGiven

import NamedTuple.AnyNamedTuple
import scala.annotation.implicitNotFound

sealed trait SiteTheme:
  type Out <: AnyNamedTuple

sealed trait SiteThemeComputed[Ctx <: AnyNamedTuple] extends SiteTheme:
  outer =>
  final type Res = SiteTheme { type Out = outer.Out }
  final def res: Res = this

// sealed trait SiteTheme[In <: AnyNamedTuple]:
//   type Out <: AnyNamedTuple
//   def cfg: In

import staticsitegen.{Doc, Docs, Ref, Layout, Article, Articles, and, ref}

class Page[T] extends Selectable:
  type Fields = NamedTuple.From[T]
  def selectDynamic(name: String): Any = ???


class Docs2[T]
class Doc2[T]
type DocMap[T] = T match
  case Doc[t]  => Doc2[t]
  case Docs[t] => Docs2[t]

type UnliftRef[T] = T match
  case Ref[t] => t

object SiteTheme:

  private object Empty extends SiteThemeComputed[AnyNamedTuple]:
    type Out = AnyNamedTuple

  def emptyTheme: SiteThemeComputed[AnyNamedTuple] = Empty

  inline def apply[Ctx <: AnyNamedTuple](ctx: Ctx)(using theme: SiteThemeComputed[Ctx]): theme.Res = theme.res

  inline given compute: [Ctx <: AnyNamedTuple]
    => (zMeta: Substructural.Zoom[(metadata: Substructural.Z), Ctx])
    => (zSite: Substructural.Zoom[(site: Substructural.Z), Ctx])
    => (zExtras: Substructural.Zoom[(extras: Substructural.Z), Ctx])
    => (zNav: Substructural.Zoom[(nav: Substructural.Z), Ctx])
    => (site0: Substructural.MapLeaves[zSite.Out & AnyNamedTuple, DocMap] )
    => (nav0: Substructural.MapLeaves[zNav.Out & AnyNamedTuple, [T] =>> DocMap[UnliftRef[T]]] )
    => (SiteThemeComputed[Ctx] { type Out = (
      metadata: zMeta.Out,
      site: site0.Out,
      extras: zExtras.Out,
      nav: nav0.Out
    ) }) = emptyTheme.asInstanceOf[SiteThemeComputed[Ctx] { type Out = (
      metadata: zMeta.Out,
      site: site0.Out,
      extras: zExtras.Out,
      nav: nav0.Out
    ) }]

trait Cursor[T] extends Selectable:
  final type Fields = NamedTuple.Map[T & AnyNamedTuple, Cursor]
  final type AtLeaf = NotGiven[T <:< AnyNamedTuple]
  def focus(using @implicitNotFound("Cannot focus on a non-leaf node") ev: AtLeaf): T
  def selectDynamic(name: String): Cursor[?]

trait LayoutX[T, Ctx <: AnyNamedTuple] extends Layout[T]:
  def render(page: Page[T], ctx: Cursor[Ctx]): String

object LayoutX:
  def apply[Ctx <: AnyNamedTuple](using Ref[Ctx])(using theme: SiteThemeComputed[Ctx])[T](f: (Page[T], Cursor[theme.Out]) => String): Layout[T] =
    new Layout[T] {
      def render(page: Page[T], ctx: Cursor[theme.Out]): String = f(page, ctx)
    }

val BreezeTheme =
  (
    metadata = (
      name = "Breeze",
      author = "github.com/bishabosha"
    ),
    site = (
      articles = (index = Doc[Articles], pages = Docs[Article]),
    ),
    extras = (
      extraHead = Seq.empty[String],
      extraFoot = Seq.empty[String]
    )
  )
    .and:
      (
        nav = (Articles = ref.site.articles.index)
      )

val themeX = SiteTheme(BreezeTheme)

def article(page: Page[Article], ctx: Cursor[themeX.Out]): String =
  s"""
  <nav>${ctx.nav.Articles.focus}</nav>
  <h1>${page.title}</h1>
  <p>${page.description}</p>
  <p>${page.published}</p>
  <ul>${ctx.site.articles.index.focus}</ul>
  <footer>${ctx.extras.extraFoot.focus}</footer>
  """

// TODO: should there be this intersection?
// perhaps site config is just independent - accepting layouts,
// input directory? output directory? etc.
val BreezeSite = BreezeTheme.and:
    (
      layouts = (
        article = LayoutX(article)
      ),
    )

def demo =

  // val finalConfig = BreezeTheme.and:
  //   (
  //     layout = LayoutX(article2),
  //   )

  ???
