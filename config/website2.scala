package staticsitegenv2

import confignt.cursor.Cursor

// import staticsitegen.NuConfig.BreezeTheme
import substructural.Sub.Substructural
import scala.util.NotGiven

import NamedTuple.AnyNamedTuple
import NamedTuple.NamedTuple
import scala.annotation.implicitNotFound

import staticsitegen.{Doc, Docs, Ref, Layout, Article, Articles, and, ref}
import substructural.Sub.Substructural.NTMirror
import substructural.Sub.Substructural.Encoded

class Page[T]:
  val frontMatter: Cursor[T] = ???

class Docs2[T]
class Doc2[T]
type DocMap[T] = T match
  case Doc[t]  => Doc2[t]
  case Docs[t] => Docs2[t]

type UnliftRef[T] = T match
  case Ref[t] => t

type ZipMaskEncoded[Mask <: AnyNamedTuple, Encoded <: Substructural.NTMirror, F[_, _]] =
  (Mask, Encoded) match
    case (NamedTuple[ns1, vs1], Substructural.NTEncoded[ns2, vs2]) =>
      ZipMaskLoop[ns1, vs1, ns2, vs2, F, EmptyTuple, EmptyTuple]
    // case _ => AnyNamedTuple

type ZipMaskLoop[Ns <: Tuple, Vs <: Tuple, ENs <: Tuple, EVs <: Tuple, F[
    _,
    _
], AccNs <: Tuple, AccVs <: Tuple] <: AnyNamedTuple = (Ns, Vs) match
  case (n *: ns, v *: vs) =>
    ZipLookup[n, v, ENs, EVs, F] match
      case Some[t] => ZipMaskLoop[ns, vs, ENs, EVs, F, n *: AccNs, t *: AccVs]
      case _       => AnyNamedTuple
  case (EmptyTuple, EmptyTuple) => NamedTuple[Tuple.Reverse[AccNs], Tuple.Reverse[AccVs]]
  case _                        => AnyNamedTuple

// type ZipLookup[N <: Tuple, V <: Tuple, ENs <: Tuple, EVs <: Tuple, F[_,_]] <: Option[Any] = (ENs, EVs) match
//   case (N *: _, v *: _) => Some[F[V, v]]
//   case (_ *: ns, _ *: vs) => ZipLookup[N, V, ns, vs, F]
//   case _ => None.type

type ZipLookup[N, V, ENs <: Tuple, EVs <: Tuple, F[_, _]] <: Option[Any] = Lookup[N, ENs, EVs] match
  case Some[v] => Some[F[V, v]]
  case _       => None.type

type Lookup[N, Ns <: Tuple, Vs <: Tuple] <: Option[Any] = (Ns, Vs) match
  case (N *: _, v *: _)   => Some[v]
  case (_ *: ns, _ *: vs) => Lookup[N, ns, vs]
  case _                  => None.type

type IsNTEncoded[Encoded] <: Boolean = Encoded match
  case Substructural.NTEncoded[ns2, vs2] => true
  case _                                 => false

sealed trait NTParserTop
sealed trait NTParserFields extends NTParserTop with Selectable:
  type Fields <: AnyNamedTuple

type Decode[Encoded <: NTMirror] <: AnyNamedTuple = Encoded match
  case Substructural.NTEncoded[ns, vs] => NamedTuple[ns, Tuple.Map[vs, [v] =>> Decode[v]]]

type MapLeaves[Encoded <: NTMirror, F[_]] = MapLeavesInternal[Encoded, F] & NTMirror

type MapLeavesInternal[Encoded, F[_]] = Encoded match
  case Substructural.NTEncoded[ns, vs] => Substructural.NTEncoded[ns, Tuple.Map[vs, F]]

final class NTTreeParser[Ctx <: AnyNamedTuple, F[_] <: Boolean, Encoded <: Substructural.NTMirror]
    extends NTParserTop:
  def yesIAmATreeParser: Int = 23
  def mapLeaves[F[_]](f: [T] => (t: T) => F[T])(
      ctx: Cursor[Ctx]
  ): Cursor[Decode[MapLeaves[Encoded, F]]] = ???

final class NTParserLeaf[T] extends NTParserTop:
  def yesIAmALeaf: Int = 23

final class NTParser[Ctx <: AnyNamedTuple, Mask <: AnyNamedTuple, Encoded <: Substructural.NTMirror]
    extends NTParserFields:
  final type Fields = NTParser.CalcFields2[this.type]

  inline def selectDynamic[N <: String & Singleton](name: N): Any =
    inline compiletime.erasedValue[NTParser.Select[N, NTParser.CalcFields2[this.type]]] match
      case _: NTParser[c, m, e] =>
        println("parser")
        new NTParser()
      case _: NTTreeParser[c, f, e] =>
        println("tree-parser")
        new NTTreeParser()
      case _: Cursor[t] =>
        println("leaf")
        Cursor(NamedTuple.Empty)

type IsNamedTuple[T <: AnyNamedTuple] = T

object NTParser:

  type CalcFields[P] <: AnyNamedTuple = P match
    case NTParser[ctx, mask, encoded] =>
      ZipMaskEncoded[mask, encoded, [M, E] =>> NTParser.MapSub[ctx, M, E]]

  type CalcFields2[P] <: AnyNamedTuple = P match
    case NTParser[ctx, mask, encoded] =>
      NamedTuple.Map[mask, [M] =>> NTParser.MapSub2[ctx, M, encoded]]

  // enum Tag[N <: String & Singleton]:
  //   case Leaf()
  //   case Tree()
  //   case Node()

  type Select[N <: String, Fields <: AnyNamedTuple] <: Any = Fields match
    case NamedTuple[ns, vs] =>
      Lookup[N, ns, vs] match
        case Some[t] => t
    // case _ => Nothing

  type SelectEncoded[N <: String, Encoded <: Substructural.NTMirror] <: Any = Encoded match
    case Substructural.NTEncoded[ns, vs] =>
      Lookup[N, ns, vs] match
        case Some[t] => t
    // case _ => Nothing
    // case _ => None.type

  type Extract[Fields <: AnyNamedTuple] <: (Tuple, Tuple) | Null = Fields match
    case NamedTuple[ns, vs] => (ns, vs)
    case _                  => Null

  type MapSub[Ctx <: AnyNamedTuple, Mask, Encoded0] <: NTParserTop = IsNTEncoded[Encoded0] match
    case true =>
      Mask match
        case NamedTuple[ns, vs] => MapInner[Ctx, ns, vs, Mask, Encoded0]
    case false => NTParserLeaf[Encoded0]

  type MapSub2[Ctx <: AnyNamedTuple, SubMask, Encoded0] = SubMask match
    case NamedTuple[ns, vs] => MapInner2[Ctx, ns, vs, SubMask, Encoded0]

  type MapInner[
      Ctx <: AnyNamedTuple,
      Ns <: Tuple,
      Vs <: Tuple,
      SubMask <: AnyNamedTuple,
      Encoded0
  ] <: NTParserTop = Ns match
    case Substructural.Keys.IsTreeOf *: EmptyTuple =>
      Vs match
        case Substructural.Keys.Predicate[f] *: EmptyTuple =>
          NTTreeParser[Ctx, f, Encoded0]
    case Substructural.Keys.IsExact *: EmptyTuple =>
      Vs match
        case t *: EmptyTuple =>
          NTParserLeaf[t]
    case _ => NTParser[Ctx, SubMask, Encoded0]

  type MapInner2[
      Ctx <: AnyNamedTuple,
      Ns <: Tuple,
      Vs <: Tuple,
      SubMask <: AnyNamedTuple,
      Encoded0
  ] = Ns match
    case Substructural.Keys.IsTreeOf *: EmptyTuple =>
      Vs match
        case Substructural.Keys.Predicate[f] *: EmptyTuple =>
          NTTreeParser[Ctx, f, Encoded0]
    case Substructural.Keys.IsExact *: EmptyTuple =>
      Vs match
        case t *: EmptyTuple =>
          Cursor[t]
    case _ => NTParser[Ctx, SubMask, Encoded0]

  def from[Mask <: AnyNamedTuple]()[Ctx <: AnyNamedTuple](c: Cursor[Ctx])(using
      ev: Substructural.HasRequirements[Mask, Ctx]
  ): NTParser[Ctx, Mask, ev.Encoded] =
    NTParser[Ctx, Mask, ev.Encoded]()

  def of[Mask <: AnyNamedTuple, Encoded0 <: Substructural.NTMirror]
      : NTParser[AnyNamedTuple, Mask, Encoded0] =
    NTParser[AnyNamedTuple, Mask, Encoded0]()

sealed trait SiteTheme[T <: AnyNamedTuple]:
  final type Ctx = T
  def cursor: Cursor[Ctx]

object SiteTheme:

  final class Parsed[Ctx <: AnyNamedTuple, OutRes <: AnyNamedTuple](
      f: Cursor[Ctx] => Cursor[OutRes]
  ) extends SiteThemeProvider[Ctx]:
    type Out = OutRes
    def parse(ctx: Cursor[Ctx]): SiteTheme[Out] = Terminal(f(ctx))

  final class Terminal[Ctx <: AnyNamedTuple](val cursor: Cursor[Ctx]) extends SiteTheme[Ctx]

  inline def apply[Ctx <: AnyNamedTuple](cursor: Cursor[Ctx])(using
      theme: SiteThemeProvider[Ctx]
  ): SiteTheme[theme.Out] =
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
      metadata: Substructural.IsExact[(name: String, author: String)],
      extras: Substructural.IsExact[(extraHead: Seq[String], extraFoot: Seq[String])],
      site: Substructural.IsTreeOf[IsDoc],
      nav: Substructural.IsTreeOf[IsDocRef]
  )

  // TODO:
  // selectable based on the Mask (which should be able to use match types because its encoded)
  // then each field selection gives you an operation that can type safe extract a sub-Cursor?
  // operation will require the Requires proof to be passed in.

  transparent inline given compute: [Ctx <: AnyNamedTuple]
    => (
        maskedBy: Substructural.Requires[ThemeInMask, Ctx]
    ) // TODO: requirements type - so left is lower bound of right
    => (zMeta: Substructural.ZoomWithOut[(metadata: Substructural.Z), Ctx])
    => (zSite: Substructural.ZoomWithOut[(site: Substructural.Z), Ctx])
    => (zExtras: Substructural.ZoomWithOut[(extras: Substructural.Z), Ctx])
    => (zNav: Substructural.ZoomWithOut[(nav: Substructural.Z), Ctx])
    => (site0: Substructural.MapLeaves[zSite.Out & AnyNamedTuple, DocMap])
    => (nav0: Substructural.MapLeaves[zNav.Out & AnyNamedTuple, [T] =>> DocMap[UnliftRef[T]]])
    => (SiteThemeProvider[Ctx] {
      type Out = (
          metadata: zMeta.Out,
          site: site0.Out,
          extras: zExtras.Out,
          nav: nav0.Out
      )
    }) = Parsed[
    Ctx,
    (
        metadata: zMeta.Out,
        site: site0.Out,
        extras: zExtras.Out,
        nav: nav0.Out
    )
  ](_.asInstanceOf)

trait LayoutX[T, Ctx <: AnyNamedTuple] extends Layout[T]:
  def render(page: Page[T], ctx: Cursor[Ctx]): String

object LayoutX:
  def apply[Ctx <: AnyNamedTuple](using
      Ref[Ctx]
  )(using theme: SiteThemeProvider[Ctx])[T](f: (Page[T], Cursor[theme.Out]) => String): Layout[T] =
    new Layout[T] {
      def render(page: Page[T], ctx: Cursor[theme.Out]): String = f(page, ctx)
    }

// TODO:
// - consider making each "concrete" leaf is also NT encoded
// - so instead of these DSL typed placeholders, we can have some "provider" or "provided" types.
// - that the config parser needs to provide.
// - ref.foo => ref.foo.provided can be "provided" (by wrapping in special NT object)
val BreezeTheme =
  (
    metadata = (
      name = "Breeze",
      author = "github.com/bishabosha"
    ),
    site = (
      articles = (index = Doc[Articles], pages = Docs[Article])
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

def SiteParser[Ctx <: AnyNamedTuple](c: Cursor[Ctx])(using
    ev: Substructural.HasRequirements[SiteThemeProvider.ThemeInMask, Ctx]
) =
  val pBreeze = NTParser[Ctx, SiteThemeProvider.ThemeInMask, ev.Encoded]()
  val qBreeze1 =
    pBreeze.site.yesIAmATreeParser // TODO: tree is not practical - can have a 1 level depth dictionary of submask
  val qBreeze2 = pBreeze.nav.yesIAmATreeParser
  val qBreeze3a = pBreeze.extras.extraHead.focus
  val qBreeze3b = pBreeze.extras.extraFoot.focus
  val qBreeze4a = pBreeze.metadata.author.focus
  val qBreeze4b = pBreeze.metadata.name.focus
