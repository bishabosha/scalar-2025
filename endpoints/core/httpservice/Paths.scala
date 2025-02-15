package serverlib.httpservice

object Paths:
  enum UriParts:
    case Exact(str: String)
    case Wildcard(name: String)

  def uriPattern(route: String): IndexedSeq[UriParts] =
    assert(route.startsWith("/"))
    val parts = route.split("/").view.drop(1)
    assert(parts.forall(_.nonEmpty))
    parts.toIndexedSeq.map {
      case s if s.startsWith("{") && s.endsWith("}") =>
        UriParts.Wildcard(s.slice(1, s.length - 1))
      case s => UriParts.Exact(s)
    }
  end uriPattern
