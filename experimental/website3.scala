package staticsitegenv3

import staticsitegen.and
import staticsitegen.ref
import staticsitegen.{Article, Articles}

import scala.NamedTuple.AnyNamedTuple
import staticsitegenv2.article

final class Req[T]
type Provided[T] = (`special.Provided`: T)
type Required[T] = (`special.Required`: Req[T])
object Required:
  def apply[T]: Required[T] = (`special.Required` = Req[T])
extension [T](x: T) def provided: Provided[T] = (`special.Provided` = x)

// TODO: this is the final Doc/Docs/Layout
trait Doc[T <: AnyNamedTuple]
trait Docs[T <: AnyNamedTuple]
trait Layout[T <: AnyNamedTuple]

val BreezeTheme =
  (
    metadata = (
      name = "Breeze".provided,
      author = "github.com/bishabosha".provided
    ),
    site = (
      articles = (index = Required[Doc[Articles]], pages = Required[Docs[Article]]),
    ),
    extras = (
      extraHead = Seq.empty[String].provided,
      extraFoot = Seq.empty[String].provided
    ),
    layouts = (
      article = Required[Layout[Article]],
      articles = Required[Layout[Articles]]
    )
  )
    .and:
      (
        nav = (Articles = ref.site.articles.index.provided),
      )
