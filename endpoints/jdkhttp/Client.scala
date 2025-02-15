package serverlib.jdkhttp

import java.net.http.HttpClient
import java.net.http.HttpRequest
import java.net.http.HttpClient.Version
import java.net.http.HttpClient.Redirect
import java.net.http.HttpResponse
import java.net.URI

import serverlib.httpservice.HttpService.Endpoints.Endpoint
import serverlib.httpservice.HttpService.Empty
import serverlib.httpservice.HttpService.Input
import serverlib.httpservice.HttpService.model.source
import serverlib.httpservice.HttpService.model.method
import serverlib.httpservice.Paths

import scala.NamedTuple.{AnyNamedTuple, DropNames}
import PartialRequest.{Request, Bundler}

class PartialRequest[I <: AnyNamedTuple, E, O](
    e: Endpoint[I, E, O],
    baseURI: String,
    builder: HttpRequest.Builder
)(using Bundler[I]):

  private val optBody: Option[IArray[String] => String] =
    e.inputs.view
      .map(_.source)
      .zipWithIndex
      .collectFirst({ case (source.body(), i) => bundle => bundle(i) })

  private val (httpMethod, route) = e.route match
    case method.get(route)  => ("GET", route)
    case method.post(route) => ("POST", route)
    case method.put(route)  => ("PUT", route)

  private val uriParts: Seq[IArray[String] => String] =
    val uriParams: Map[String, Int] =
      e.inputs.zipWithIndex
        .collect({ case (Input(label, source.path()), i) =>
          (label, i)
        })
        .toMap
    Paths
      .uriPattern(route)
      .map({
        case Paths.UriParts.Exact(str) => Function.const(str)
        case Paths.UriParts.Wildcard(name) =>
          val i = uriParams(name)
          bundle => bundle(i)
      })
  end uriParts

  protected def handle(bundle: IArray[String]): Request[E, O] =
    val uri = uriParts.view.map(_(bundle)).mkString(baseURI, "/", "")
    val withUri = builder.uri(URI.create(uri))
    val withBody = optBody.fold(withUri)(get =>
      val body = get(bundle)
      assert(body.nonEmpty, "empty body")
      withUri
        .setHeader("Content-Type", "text/plain")
        .method(httpMethod, HttpRequest.BodyPublishers.ofString(body))
    )
    Request(withBody.build())
  end handle

  def prepare(input: I): Request[E, O] = handle(summon[Bundler[I]].bundle(input))
end PartialRequest

object PartialRequest:

  class Request[E, O] private[PartialRequest] (req: HttpRequest):
    protected def baseSend =
      HttpClient.newHttpClient().send(req, HttpResponse.BodyHandlers.ofString())

    def sendWithError()(using Des[E], Des[O]): Either[E, O] =
      val response = baseSend
      val body = response.body()
      if response.statusCode() < 400 then Right(summon[Des[O]].deserialize(body))
      else Left(summon[Des[E]].deserialize(body))
    end sendWithError

    def sendNoError()(using Des[O]): O =
      val response = baseSend
      if response.statusCode() < 400 then summon[Des[O]].deserialize(response.body())
      else
        throw new RuntimeException(
          s"Request failed with status ${response.statusCode()} and body ${response.body()}"
        )
      end if
    end sendNoError

    inline def send()(using Des[E], Des[O]): Res[E, O] = inline compiletime.erasedValue[E] match
      case _: Empty => sendNoError()
      case _        => sendWithError()
  end Request

  type Res[E, O] = E match
    case Empty => O
    case _     => Either[E, O]

  trait Bundler[I <: AnyNamedTuple]:
    def bundle(i: I): IArray[String]

  trait Des[T]:
    def deserialize(s: String): T

  object Des:
    given Des[String] with
      def deserialize(s: String): String = s
    given Des[Int] with
      def deserialize(s: String): Int = s.toInt

    given Des[Unit] with
      def deserialize(s: String): Unit = ()

    given Des[Empty] with
      def deserialize(s: String): Empty = ??? // should never be called
  end Des

  trait Ser[I]:
    def serialize(i: I): String

  object Ser:
    given Ser[String] with
      def serialize(i: String): String = i
    given Ser[Int] with
      def serialize(i: Int): String = i.toString
  end Ser

  object Bundler:
    final class NTBundler[I <: AnyNamedTuple](serializers: => Tuple) extends Bundler[I]:
      lazy val sers = serializers.toIArray.map(_.asInstanceOf[Ser[Any]])
      def bundle(i: I): IArray[String] = IArray.from(
        i.asInstanceOf[Tuple]
          .productIterator
          .zip(sers.iterator)
          .map({ case (a, ser) =>
            ser.serialize(a)
          })
      )
    end NTBundler

    inline given [I <: AnyNamedTuple]: Bundler[I] =
      NTBundler[I](compiletime.summonAll[Tuple.Map[DropNames[I], Ser]])

  end Bundler

  inline def apply[I <: AnyNamedTuple, E, O](e: Endpoint[I, E, O], baseURI: String)(using
      Bundler[I]
  ): PartialRequest[I, E, O] =
    new PartialRequest(e, s"$baseURI/", HttpRequest.newBuilder())
end PartialRequest
