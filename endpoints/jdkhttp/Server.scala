package serverlib.jdkhttp

import language.experimental.namedTuples

import com.sun.net.httpserver.HttpHandler
import com.sun.net.httpserver.HttpExchange
import com.sun.net.httpserver.HttpServer

import java.util.concurrent.Executors
import scala.collection.mutable.ListBuffer

import serverlib.httpservice.HttpService.Empty
import serverlib.httpservice.HttpService.Endpoints
import serverlib.httpservice.HttpService.Endpoints.Endpoint
import serverlib.httpservice.HttpService
import serverlib.httpservice.HttpService.Route
import serverlib.httpservice.Paths.*

import scala.util.TupledFunction

import scala.NamedTuple.AnyNamedTuple
import scala.NamedTuple.NamedTuple
import scala.NamedTuple.DropNames

import serverlib.util.optional
import scala.collection.View.Empty
import mirrorops.OpsMirror
import serverlib.httpservice.HttpService.special
import scala.util.Try

class Server private (private val internal: HttpServer) extends AutoCloseable:
  def close(): Unit = internal.stop(0)

object Server:

  enum HttpMethod:
    case Get, Post, Put, Delete

  type UriHandler = String => Option[Map[String, String]]

  type Res[E, O] = E match
    case Empty => O
    case _     => Either[E, O]

  trait Exchanger[I <: AnyNamedTuple, E, O](using Ser[Res[E, O]]):
    def apply(bundle: Bundle, func: I => Res[E, O]): Ser.Result

  trait Ser[O]:
    def serialize(o: O): Ser.Result

  object Ser:
    val jsonKind = "application/json; charset=utf-8"
    type Side = Option[(String, Array[Byte])]
    type Result = Either[Side, Side]

    given [E: Ser, O: Ser] => Ser[Either[E, O]]:
      def serialize(o: Either[E, O]): Result = o.fold(
        e => Left(summon[Ser[E]].serialize(e).merge),
        o => Right(summon[Ser[O]].serialize(o).merge)
      )
    end given

    given Ser[Unit]:
      def serialize(o: Unit): Result = Right(None)

    given Ser[special.Static]:
      def serialize(o: special.Static): Result = Right(
        Some(("<INFER>", o.render))
      )

    given Ser[String]:
      def serialize(o: String): Result = Right(
        Some((jsonKind, ('"' + o + '"').getBytes(java.nio.charset.StandardCharsets.UTF_8)))
      )
    end given

    given Ser[Int]:
      def serialize(o: Int): Result = Right(
        Some((jsonKind, o.toString.getBytes(java.nio.charset.StandardCharsets.UTF_8)))
      )
  end Ser

  trait Des[I]:
    def deserialize(str: String): I

  object Des:
    given Des[Int] with
      def deserialize(str: String): Int = str.toInt

    given Des[String] with
      def deserialize(str: String): String = str
  end Des

  object Exchanger:
    final class NTExchanger[I <: AnyNamedTuple, E, O](deserializers: => Tuple)(using
        Ser[Res[E, O]]
    ) extends Exchanger[I, E, O]:
      lazy val dess = deserializers.toIArray.map(_.asInstanceOf[Des[Any]]).zipWithIndex
      def apply(bundle: Bundle, func: I => Res[E, O]): Ser.Result =
        val i: I = Tuple
          .fromIArray(
            dess.map: (d, idx) =>
              d.deserialize(bundle.arg(idx))
          )
          .asInstanceOf[I]
        summon[Ser[Res[E, O]]].serialize(func(i))
      end apply

    inline given [I <: AnyNamedTuple, E, O](using Ser[Res[E, O]]): Exchanger[I, E, O] =
      NTExchanger[I, E, O](compiletime.summonAll[Tuple.Map[DropNames[I], Des]])
  end Exchanger

  trait Bundle:
    def arg(index: Int): String

  type Handlers[I <: AnyNamedTuple] =
    NamedTuple.Map[
      I,
      [X] =>> X match
        case Endpoint[ins, err, out] => Handler[ins, err, out]
    ]

  type HandlerFuncs[I <: AnyNamedTuple] =
    NamedTuple.From[
      NamedTuple.Map[
        I,
        [X] =>> X match
          case Endpoint[ins, err, out] => (ins => Res[err, out])
      ]
    ]

  type HandlerExchangers[I <: AnyNamedTuple] =
    Tuple.Map[
      NamedTuple.DropNames[I],
      [X] =>> X match
        case Endpoint[ins, err, out] => Exchanger[ins, err, out]
    ]

  private def rootHandler(handlers: List[Handler[?, ?, ?]]): HttpHandler =
    val lazyHandlers = handlers
      .to(LazyList)
      .map: h =>
        val (method, uriHandler) = h.route
        method -> (uriHandler, h)
      .groupMap: (method, _) =>
        method
      .apply: (_, pair) =>
        pair

    def makeExchange(exchange: HttpExchange): Unit =
      // get method
      val method = exchange.getRequestMethod match
        case "GET"    => HttpMethod.Get
        case "POST"   => HttpMethod.Post
        case "PUT"    => HttpMethod.Put
        case "DELETE" => HttpMethod.Delete
        case _        => throw new IllegalArgumentException("Unsupported method")

      // get uri
      val uri = exchange.getRequestURI.getPath()
      // match the uri to a handler
      val handlerOpt = lazyHandlers
        .get(method)
        .flatMap: ls =>
          ls
            .flatMap: (uriHandler, h) =>
              uriHandler(uri).map: params =>
                h -> params
            .headOption

      def readBody(length: Int): Array[Byte] =
        // consume the full input stream
        val is = exchange.getRequestBody
        try
          is.readAllBytes()
            .ensuring(_.length == length, "read less bytes than expected")
        finally
          is.close()
      end readBody

      def readKind(kind: String): String =
        if kind == "<INFER>" then
          if uri.endsWith(".html") || uri.endsWith("/") then "text/html; charset=utf-8"
          else if uri.endsWith(".css") then "text/css; charset=utf-8"
          else if uri.endsWith(".js") then "application/javascript; charset=utf-8"
          else "text/plain; charset=utf-8"
        else kind

      try
        handlerOpt match
          case None =>
            exchange.sendResponseHeaders(404, -1)
          case Some((handler, params)) =>
            val length =
              Option(exchange.getRequestHeaders.getFirst("Content-Length"))
                .map(_.toInt)
                .getOrElse(0)
            val body = readBody(length)
            val bodyStr =
              new String(body, java.nio.charset.StandardCharsets.UTF_8)
            println(
              s"matched ${uri} to handler ${handler.debug} with params ${params}\nbody: ${bodyStr}"
            )

            val attempt = Try(handler.exchange(params, bodyStr))

            val res = attempt match
              case scala.util.Success(value) => value
              case scala.util.Failure(exception) =>
                println(s"error: ${exception}")
                Left(None)


            res match
              case Left(errExchange) =>
                // TODO: in real world you would encode the data type to the error format
                errExchange match
                  case None =>
                    println("debug: err no response [500]")
                    exchange.sendResponseHeaders(500, -1)
                  case Some((kind, response)) =>
                    println(s"debug: err yes response [500] ${response.length}")
                    exchange.getResponseHeaders.add("Content-Type", readKind(kind))
                    exchange.sendResponseHeaders(500, response.length)
                    exchange.getResponseBody.write(response)
              case Right(response) =>
                response match
                  case None =>
                    println("debug: no response [200]")
                    exchange.sendResponseHeaders(200, -1)
                  case Some((kind, response)) =>
                    println(s"debug: yes response [200] ${response.length}")
                    exchange.getResponseHeaders.add("Content-Type", readKind(kind))
                    exchange.sendResponseHeaders(200, response.length)
                    exchange.getResponseBody.write(response)
            end match
      finally
        exchange.close()
      end try
    end makeExchange

    makeExchange(_)
  end rootHandler

  class Handler[I <: AnyNamedTuple, E, O](
      routeName: String,
      e: Endpoint[I, E, O],
      op: I => Res[E, O],
      exchange: Exchanger[I, E, O]
  ):
    import serverlib.httpservice.HttpService.model.*

    type Bundler = (params: Map[String, String], body: String) => Bundle
    type BundleArg = (params: Map[String, String], body: String) => String

    val template: Bundler =
      val readers: Array[BundleArg] = e.inputs
        .map[BundleArg]: i =>
          (i.source: @unchecked) match
            case source.path() =>
              val name = i.label
              (params, _) => params(name)
            case source.body() =>
              (_, body) => body
        .toArray
      (params, body) =>
        new:
          def arg(index: Int): String = readers(index)(params, body)
    end template

    def exchange(params: Map[String, String], body: String): Ser.Result =
      val bundle = template(params, body)
      exchange(bundle, op)

    def uriHandle(route: String): UriHandler =
      val elems = uriPattern(route)
      uri =>
        optional:
          val uriElems = uri.split("/")
          val elemsIt = elems.iterator
          val uriIt = uriElems.iterator.filter(_.nonEmpty)
          var result = Map.empty[String, String]
          while elemsIt.hasNext && uriIt.hasNext do
            elemsIt.next() match
              case UriParts.Exact(str) =>
                if uriIt.next() != str then optional.abort
              case UriParts.Wildcard(name) =>
                result += (name -> uriIt.next())
          end while
          if elemsIt.hasNext || uriIt.hasNext then optional.abort
          result
    end uriHandle

    def debug: String = e.route match
      case method.get(route)    => s"$routeName: GET ${route}"
      case method.post(route)   => s"$routeName: POST ${route}"
      case method.put(route)    => s"$routeName: PUT ${route}"
      case method.delete(route) => s"$routeName: DELETE ${route}"

    def route: (HttpMethod, UriHandler) = e.route match
      case method.get(route)    => (HttpMethod.Get, uriHandle(route))
      case method.post(route)   => (HttpMethod.Post, uriHandle(route))
      case method.put(route)    => (HttpMethod.Put, uriHandle(route))
      case method.delete(route) => (HttpMethod.Delete, uriHandle(route))

  end Handler

  class effestion[Service, I <: AnyNamedTuple](e: Endpoints[Service] { type Fields = I }):
    type Routes = HandlerFuncs[I]

    class Butler(handlers: List[Handler[?, ?, ?]]):
      def listen(port: Int): Server = ServerBuilder.serve(port, handlers)

    inline def handle(r: Routes): Butler =
      Butler(ServerBuilder.mkHandlers(e.routes, r))
  end effestion

  object ServerBuilder:
    def serve(port: Int, handlers: List[Handler[?, ?, ?]]): Server =
      // Log the added handlers for debugging
      println(
        s"adding handles ${handlers.map(_.debug).mkString("\n> ", "\n> ", "")}"
      )
      val server = HttpServer.create()
      val handlers0 = handlers.toList
      server.bind(new java.net.InetSocketAddress(port), 0)
      val _ = server.createContext("/", rootHandler(handlers0))
      server.setExecutor(Executors.newVirtualThreadPerTaskExecutor())
      server.start()
      println(s"serving at http://localhost:$port")
      Server(server)

    def handlersFromTup[I <: AnyNamedTuple](namesTuple: Tuple, exchangers: Tuple, e: Map[String, HttpService.Route], impls: HandlerFuncs[I]): List[Handler[?,?,?]] =
      type N0 <: String
      type I0 <: AnyNamedTuple
      type E0
      type O0
      impls
        .asInstanceOf[Product]
        .productIterator
        .zip(namesTuple.productIterator.asInstanceOf[Iterator[N0]])
        .zip(exchangers.productIterator)
        .map({ case ((func, name), exchanger) =>
          val endpoint =
            e(name).asInstanceOf[Endpoint[I0, E0, O0]]
          val op = func.asInstanceOf[I0 => Res[E0, O0]]
          val ex = exchanger.asInstanceOf[Exchanger[I0, E0, O0]]
          Handler(name, endpoint, op, ex)
        })
        .toList

    transparent inline def mkHandlers[I <: AnyNamedTuple](e: Map[String, HttpService.Route], impls: HandlerFuncs[I]): List[Handler[?,?,?]] =
      val namesTuple = compiletime.constValueTuple[NamedTuple.Names[I]]
      val exchangers = compiletime.summonAll[HandlerExchangers[I]]
      handlersFromTup(namesTuple, exchangers, e, impls)


  class ServerBuilder():
    private val handlers: ListBuffer[Handler[?, ?, ?]] = ListBuffer()

    transparent inline def addEndpoints[Service, I <: AnyNamedTuple](
        e: Endpoints[Service] { type Fields = I }
    )(impls: HandlerFuncs[I]): this.type =
      handlers ++= ServerBuilder.mkHandlers(e.routes, impls)
      this
    end addEndpoints

    def create(port: Int): Server = ServerBuilder.serve(port, handlers.toList)
  end ServerBuilder
end Server
