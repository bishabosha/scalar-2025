package configschema

import NamedTuple.AnyNamedTuple

val schema = Frontmatter.schema[
  (
      title: String,
      published: String,
      startDate: String,
      avatar: String,
      links: List[String],
      name: String,
      copyright: String,
      subtitle: String,
      url: String,
      description: String,
      isIndexOnly: Boolean,
      isInProgress: Boolean,
      ordered: String
  )
]

val config = (
  site = schema.sitemap[
    (
        about: schema.Doc,
        articles: schema.Docs
    )
  ],
  layouts = (
    article = ???
  )
)

trait Doc extends scala.Selectable:
  type Fields <: AnyNamedTuple
trait Docs extends scala.Selectable:
  type Fields <: AnyNamedTuple

object Frontmatter:

  trait SiteMap[T <: AnyNamedTuple]

  trait Schema[T <: AnyNamedTuple]:
    type Doc = configschema.Doc { type Fields = T }
    type Docs = configschema.Docs { type Fields = T }

    def sitemap[S <: AnyNamedTuple]: SiteMap[S] = ???

  def schema[T <: AnyNamedTuple]: Frontmatter.Schema[T] = ???
