package other

// import staticsitegen.NuConfig.BreezeTheme
import substructural.Sub.Substructural
import scala.util.NotGiven

import NamedTuple.AnyNamedTuple
import NamedTuple.NamedTuple
import scala.annotation.implicitNotFound

import staticsitegen.{Doc, Docs, Ref, Layout, Article, Articles, and, ref}

class Page[T]:
  val frontMatter: Cursor[T] = ???

class Docs2[T]
class Doc2[T]
type DocMap[T] = T match
  case Doc[t]  => Doc2[t]
  case Docs[t] => Docs2[t]

type UnliftRef[T] = T match
  case Ref[t] => t

type ZipMaskEncoded[Mask <: AnyNamedTuple, Encoded <: Substructural.NTMirror, F[_,_]] <: AnyNamedTuple = (Mask, Encoded) match
  case (NamedTuple[ns1,vs1],Substructural.NTEncoded[ns2, vs2]) => ZipMaskLoop[ns1, vs1, ns2, vs2, F, EmptyTuple, EmptyTuple]
  case _ => AnyNamedTuple

type ZipMaskLoop[Ns <: Tuple, Vs <: Tuple, ENs <: Tuple, EVs <: Tuple, F[_,_], AccNs <: Tuple, AccVs <: Tuple] <: AnyNamedTuple = (Ns, Vs) match
  case (n *: ns, v *: vs) => ZipLookup[n, v, ENs, EVs, F] match
    case Some[t] => ZipMaskLoop[ns, vs, ENs, EVs, F, n *: AccNs, t *: AccVs]
    case _ => AnyNamedTuple
  case (EmptyTuple, EmptyTuple) => NamedTuple[Tuple.Reverse[AccNs], Tuple.Reverse[AccVs]]
  case _ => AnyNamedTuple

type ZipLookup[N <: Tuple, V <: Tuple, ENs <: Tuple, EVs <: Tuple, F[_,_]] <: Option[Any] = (ENs, EVs) match
  case (N *: _, v *: _) => Some[F[V, v]]
  case (_ *: ns, _ *: vs) => ZipLookup[N, V, ns, vs, F]
  case _ => None.type

type IsNTEncoded[Encoded] <: Boolean = Encoded match
  case Substructural.NTEncoded[ns2, vs2] => true
  case _ => false

sealed trait NTParserTop
sealed trait NTParserFields extends NTParserTop with Selectable:
  type Fields <: AnyNamedTuple

sealed trait NTTreeParser[F[_] <: Boolean, Encoded <: Substructural.NTMirror] extends NTParserTop:
  def yesIAmATreeParser: Int = 23
  // def mapLeaves[F[_]](f: [T] =>> (t: T) => F[T])

sealed trait NTParserLeaf[T] extends NTParserTop

final class NTParser[Mask <: AnyNamedTuple, Encoded <: Substructural.NTMirror] extends NTParserFields:
  final type Fields = ZipMaskEncoded[Mask, Encoded, NTParser.MapSub]
  def selectDynamic(name: String): NTParserTop = ???

type IsNamedTuple[T <: AnyNamedTuple] = T

object NTParser:
  type MapSub[Mask, Encoded0] <: NTParserTop = IsNTEncoded[Encoded0] match
    case true => Mask match
      case NamedTuple[ns, vs] => MapInner[ns, vs, Mask, Encoded0]
      case _ => Nothing
    case _ => NTParserLeaf[Encoded0]

  type MapInner[Ns <: Tuple, Vs <: Tuple, Mask <: AnyNamedTuple, Encoded0] <: NTParserTop = Ns match
    case Substructural.Keys.IsTreeOf *: EmptyTuple => Vs match
      case Substructural.Keys.Predicate[f] *: EmptyTuple =>
        NTTreeParser[f, Encoded0]
    case _ => NTParser[Mask, Encoded0]

  def of[Mask <: AnyNamedTuple, Encoded0 <: Substructural.NTMirror]: NTParser[Mask, Encoded0] =
    NTParser[Mask, Encoded0]()

sealed trait SiteTheme[T <: AnyNamedTuple]:
  final type Ctx = T
  def cursor: Cursor[Ctx]

object SiteTheme:

  final class Parsed[Ctx <: AnyNamedTuple, OutRes <: AnyNamedTuple](f: Cursor[Ctx] => Cursor[OutRes])
      extends SiteThemeProvider[Ctx]:
    type Out = OutRes
    def parse(ctx: Cursor[Ctx]): SiteTheme[Out] = Terminal(f(ctx))

  final class Terminal[Ctx <: AnyNamedTuple](val cursor: Cursor[Ctx]) extends SiteTheme[Ctx]

  inline def apply[Ctx <: AnyNamedTuple](cursor: Cursor[Ctx])(using theme: SiteThemeProvider[Ctx]): SiteTheme[theme.Out] =
    theme.parse(cursor)

end SiteTheme

sealed trait SiteThemeProvider[Ctx <: AnyNamedTuple]:
  outer =>
  type Out <: AnyNamedTuple
  def parse(ctx: Cursor[Ctx]): SiteTheme[Out]

object SiteThemeProvider:
  import SiteTheme.Parsed

  type IsDoc[T] <: Boolean = T match
    case Doc[_]  => true
    case Docs[_] => true

  type IsDocRef[T] <: Boolean = T match
    case Ref[t] => IsDoc[t]

  type ThemeInMask = (
    metadata: (name: String, author: String),
    extras: (extraHead: Seq[String], extraFoot: Seq[String]),
    site: Substructural.IsTreeOf[IsDoc],
    nav: Substructural.IsTreeOf[IsDocRef],
  )

  // TODO:
  // selectable based on the Mask (which should be able to use match types because its encoded)
  // then each field selection gives you an operation that can type safe extract a sub-Cursor?
  // operation will require the Requires proof to be passed in.

  transparent inline given compute: [Ctx <: AnyNamedTuple]
    => (maskedBy: Substructural.Requires[ThemeInMask, Ctx]) // TODO: requirements type - so left is lower bound of right
    => (zMeta: Substructural.ZoomWithOut[(metadata: Substructural.Z), Ctx])
    => (zSite: Substructural.ZoomWithOut[(site: Substructural.Z), Ctx])
    => (zExtras: Substructural.ZoomWithOut[(extras: Substructural.Z), Ctx])
    => (zNav: Substructural.ZoomWithOut[(nav: Substructural.Z), Ctx])
    => (site0: Substructural.MapLeaves[zSite.Out & AnyNamedTuple, DocMap] )
    => (nav0: Substructural.MapLeaves[zNav.Out & AnyNamedTuple, [T] =>> DocMap[UnliftRef[T]]] )
    => (SiteThemeProvider[Ctx] { type Out = (
      metadata: zMeta.Out,
      site: site0.Out,
      extras: zExtras.Out,
      nav: nav0.Out
    )}) = Parsed[Ctx, (
      metadata: zMeta.Out,
      site: site0.Out,
      extras: zExtras.Out,
      nav: nav0.Out
    )](???)

sealed trait Cursor[T] extends Selectable:
  final type Ref = T
  final type Fields = NamedTuple.Map[T & AnyNamedTuple, Cursor]
  final type AtLeaf = NotGiven[T <:< AnyNamedTuple]
  def focus(using @implicitNotFound("Cannot focus on a non-leaf node") ev: AtLeaf): T
  def selectDynamic(name: String): Cursor[?]

object Cursor:
  type IsNamedTuple[T] = T match
    case NamedTuple.NamedTuple[_, _] => true
    case _                           => None.type

  type ExtractNT[T] <: (Tuple, Tuple) = T match
    case NamedTuple.NamedTuple[ns, vs] => (ns, vs)

  inline def isNamedTupleType[T]: Boolean = inline compiletime.constValueOpt[IsNamedTuple[T]] match
    case Some(_) => true
    case _       => false

  inline def apply[T <: AnyNamedTuple](t: T): Cursor[T] =
    inline if isNamedTupleType[T] then applyInner[T](t)
    else compiletime.error("Cursor can only be created from a concrete NamedTuple")

  def compose[T](names: Tuple, values: List[Cursor[?]]): CursorImpl[T] =
    CursorImpl.Node(Map.from(names.productIterator.asInstanceOf[Iterator[String]].zip(values)))

  inline def applyInner[T](t: T): Cursor[T] =
    inline if isNamedTupleType[T] then
      inline compiletime.erasedValue[ExtractNT[T]] match
        case _: (ns, vs) =>
          def createNode: CursorImpl[T] =
            val keys = compiletime.constValueTuple[ns]
            val values = mapInner[vs, vs](t.asInstanceOf[vs], 0)
            compose[T](keys, values)
          createNode
    else CursorImpl.Leaf(t)

  inline def mapInner[Ts <: Tuple, Sub <: Tuple](ts: Ts, idx: Int): List[Cursor[?]] =
    inline compiletime.erasedValue[Sub] match
      case _: (s *: ss) =>
        applyInner[s](ts.productElement(idx).asInstanceOf[s]) :: mapInner[Ts, ss](ts, idx + 1)
      case _: EmptyTuple => Nil

enum CursorImpl[T] extends Cursor[T]:
  case Leaf(value: T)
  case Node(inner: Map[String, Cursor[?]])

  def focus(using ev: AtLeaf): T = this.asInstanceOf[Leaf[T]].value
  def selectDynamic(name: String): Cursor[?] = this.asInstanceOf[Node[T]].inner(name)

trait LayoutX[T, Ctx <: AnyNamedTuple] extends Layout[T]:
  def render(page: Page[T], ctx: Cursor[Ctx]): String

object LayoutX:
  def apply[Ctx <: AnyNamedTuple](using
      Ref[Ctx]
  )(using theme: SiteThemeProvider[Ctx])[T](f: (Page[T], Cursor[theme.Out]) => String): Layout[T] =
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

val breezeCursor = Cursor(BreezeTheme)

val themeX = SiteTheme(breezeCursor)

def article(page: Page[Article], ctx: Cursor[themeX.Ctx]): String =
  s"""
  <nav>${ctx.nav.Articles.focus}</nav>
  <h1>${page.frontMatter.title.focus}</h1>
  <p>${page.frontMatter.description.focus}</p>
  <p>${page.frontMatter.published.focus}</p>
  <ul>${ctx.site.articles.index.focus}</ul>
  <footer>${ctx.extras.extraFoot.focus}</footer>
  """

// TODO: should there be this intersection?
// perhaps site config is just independent - accepting layouts,
// input directory? output directory? etc.

def demo =
  // val finalConfig = BreezeTheme.and:
  //   (
  //     layout = LayoutX(article2),
  //   )

  ???
