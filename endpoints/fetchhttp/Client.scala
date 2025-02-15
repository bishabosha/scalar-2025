package serverlib.fetchhttp

import org.scalajs.dom
import scala.scalajs.js

import serverlib.httpservice.HttpService.Endpoints.Endpoint
import serverlib.httpservice.HttpService.Empty
import serverlib.httpservice.HttpService.Input
import serverlib.httpservice.HttpService.model.source
import serverlib.httpservice.HttpService.model.method
import serverlib.httpservice.Paths

import scala.NamedTuple.{AnyNamedTuple, DropNames}
import PartialRequest.{Request, Bundler}
import scala.concurrent.Future
import scala.concurrent.ExecutionContext

class PartialRequest[I <: AnyNamedTuple, E, O](
    e: Endpoint[I, E, O],
    baseURI: String
)(using Bundler[I]):

  private val optBody: Option[IArray[String] => String] =
    e.inputs.view
      .map(_.source)
      .zipWithIndex
      .collectFirst({ case (source.body(), i) => bundle => bundle(i) })

  private val (httpMethod, route) = e.route match
    case method.get(route)  => (dom.HttpMethod.GET, route)
    case method.post(route) => (dom.HttpMethod.POST, route)
    case method.put(route)  => (dom.HttpMethod.PUT, route)

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
    Request(
      new dom.Request(
        uri,
        new:
          method = httpMethod
          headers = js.Dictionary("Content-Type" -> "application/json")
          body = optBody.fold(js.undefined)(get => get(bundle))
      )
    )
  end handle

  def prepare(input: I): Request[E, O] = handle(summon[Bundler[I]].bundle(input))
end PartialRequest

object PartialRequest:

  class Request[E, O] private[PartialRequest] (req: dom.Request):
    protected def baseSend =
      dom.fetch(req).toFuture

    def sendWithError()(using ExecutionContext, Des[E], Des[O]): Future[Either[E, O]] =
      for
        response <- baseSend
        body <- response.text().toFuture
      yield
        if response.status < 400 then Right(summon[Des[O]].deserialize(body))
        else Left(summon[Des[E]].deserialize(body))
    end sendWithError

    def sendNoError()(using ExecutionContext, Des[O]): Future[O] =
      for
        response <- baseSend
        body <- response.text().toFuture
        res <-
          if response.status < 400 then Future.successful(summon[Des[O]].deserialize(body))
          else
            Future.failed(
              new RuntimeException(
                s"Request failed with status ${response.status} and body ${body}"
              )
            )
      yield res
    end sendNoError

    inline def send()(using ExecutionContext, Des[E], Des[O]): Res[E, O] =
      inline compiletime.erasedValue[E] match
        case _: Empty => sendNoError()
        case _        => sendWithError()
  end Request

  type Res[E, O] = E match
    case Empty => Future[O]
    case _     => Future[Either[E, O]]

  trait Bundler[I <: AnyNamedTuple]:
    def bundle(i: I): IArray[String]

  trait Des[T]:
    def deserialize(s: String): T

  object Des:
    given Des[String] with
      def deserialize(s: String): String = s match
        case s if s.startsWith("\"") && s.endsWith("\"") => s.drop(1).dropRight(1)
        case _ => throw new IllegalArgumentException(s"Expected json string, got $s")
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
      def serialize(i: String): String = '"' + i + '"'
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
    new PartialRequest(e, s"$baseURI/")
end PartialRequest
