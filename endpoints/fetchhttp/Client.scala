package serverlib.fetchhttp

import org.scalajs.dom
import scala.scalajs.js

import serverlib.httpservice.HttpService.Endpoints.Endpoint
import serverlib.httpservice.HttpService.Endpoints
import serverlib.httpservice.HttpService.Empty
import serverlib.httpservice.HttpService.Input
import serverlib.httpservice.HttpService.model.source
import serverlib.httpservice.HttpService.model.method
import serverlib.httpservice.Paths

import scala.NamedTuple.{AnyNamedTuple, DropNames, Names}
import PartialRequest.{Request, Bundler}
import scala.concurrent.Future
import scala.concurrent.ExecutionContext
import serverlib.fetchhttp.PartialRequest.Des
import serverlib.fetchhttp.PartialRequest.Res
import scala.util.NotGiven

object Client:
  type Bundlers[Endpoints <: AnyNamedTuple] =
    Tuple.Map[
      DropNames[Endpoints],
      [X] =>> X match
        case Endpoint[i, _, _] => Bundler[i]
    ]

  type ErrorTags[Endpoints <: AnyNamedTuple] =
    Tuple.Map[
      DropNames[Endpoints],
      [X] =>> X match
        case Endpoint[_, e, _] => TagOf[e]
    ]

  type Requests[Endpoints <: AnyNamedTuple] =
    NamedTuple.Map[
      Endpoints,
      [X] =>> X match
        case Endpoint[i, e, o] => PartialRequest[i, e, o]
    ]

  final class Client[E <: AnyNamedTuple](
      baseURI: String,
      endpoints: Endpoints[?],
      tags: => Tuple,
      names: => Tuple,
      bundlers: => Tuple
  ) extends scala.Selectable:
    type Fields = Requests[E]
    private lazy val lookup: Map[String, PartialRequest[AnyNamedTuple, Any, Any]] =
      val ns: Iterator[String] =
        names.productIterator.asInstanceOf[Iterator[String]]
      val bs: Iterator[Bundler[AnyNamedTuple]] =
        bundlers.productIterator.asInstanceOf[Iterator[Bundler[AnyNamedTuple]]]
      val ts: Iterator[PartialRequest.Tag] =
        tags.productIterator.asInstanceOf[Iterator[TagOf[Any]]].map(_.tag)
      ns.zip(ts.zip(bs)).map({ case (name, (tag, bundler)) =>
        name -> new PartialRequest(
          endpoints.selectDynamic(name).asInstanceOf[Endpoint[AnyNamedTuple, Any, Any]],
          baseURI,
          tag
        )(using bundler)
      }).toMap

    def selectDynamic(name: String): PartialRequest[AnyNamedTuple, Any, Any] =
      lookup(name)

  transparent inline def ofEndpoints[Service, I <: AnyNamedTuple](
      e: Endpoints[Service] { type Fields = I },
      baseURI: String
  ): Client[I] =
    val bundlers = compiletime.summonAll[Bundlers[I]]
    val names = compiletime.constValueTuple[Names[I]]
    val ts = compiletime.summonAll[ErrorTags[I]]
    Client(baseURI, e, ts, names, bundlers)

  trait TagOf[E]:
    def tag: PartialRequest.Tag

  object TagOf:
    given TagOf[Empty]:
      def tag: PartialRequest.Tag = PartialRequest.Tag.Empty
    given [E] => NotGiven[E =:= Empty] => TagOf[E]:
      def tag: PartialRequest.Tag = PartialRequest.Tag.NonEmpty

  // transparent inline def tags[T <: Tuple]: List[PartialRequest.Tag] =
  //   inline compiletime.erasedValue[T] match
  //     case _: (t *: ts)  => tag0[t] :: tags[ts]
  //     case _: EmptyTuple => Nil

  // transparent inline def tag0[T]: PartialRequest.Tag = inline compiletime.erasedValue[T] match
  //   case _: Endpoint[_, e, _] => tag1[e]

  // transparent inline def tag1[E]: PartialRequest.Tag = inline compiletime.erasedValue[E] match
  //   case _: Empty => PartialRequest.Tag.Empty
  //   case _        => PartialRequest.Tag.NonEmpty

class PartialRequest[I <: AnyNamedTuple, E, O](
    e: Endpoint[I, E, O],
    baseURI: String,
    tag: PartialRequest.Tag
)(using Bundler[I]):

  private val optBody: Option[IArray[String] => String] =
    e.inputs.view
      .map(_.source)
      .zipWithIndex
      .collectFirst({ case (source.body(), i) => bundle => bundle(i) })

  private val (httpMethod, route) = e.route match
    case method.get(route)    => (dom.HttpMethod.GET, route)
    case method.post(route)   => (dom.HttpMethod.POST, route)
    case method.put(route)    => (dom.HttpMethod.PUT, route)
    case method.delete(route) => (dom.HttpMethod.DELETE, route)

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
    println(s"fetching with uri: $uri")
    Request(
      new dom.Request(
        uri,
        new:
          method = httpMethod
          headers = js.Dictionary(
            "Content-Type" -> "application/json",
          )
          body = optBody.fold(js.undefined)(get => get(bundle))
      )
    )
  end handle

  def prepare(input: I): Request[E, O] = handle(summon[Bundler[I]].bundle(input))

  def send(input: I)(using ExecutionContext, Des[E], Des[O]): Res[E, O] = tag match
    case PartialRequest.Tag.Empty    => prepare(input).sendNoError().asInstanceOf[Res[E, O]]
    case PartialRequest.Tag.NonEmpty => prepare(input).sendWithError().asInstanceOf[Res[E, O]]

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

  end Request

  type Res[E, O] = E match
    case Empty => Future[O]
    case _     => Future[Either[E, O]]

  enum Tag:
    case Empty, NonEmpty

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

  // TODO: need a path serializer, separate from body serializer?
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
    new PartialRequest(
      e,
      s"$baseURI/",
      inline compiletime.erasedValue[E] match {
        case _: Empty => Tag.Empty
        case _        => Tag.NonEmpty
      }
    )
end PartialRequest
