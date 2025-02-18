package serverlib.httpservice

import mirrorops.ErrorAnnotation
import mirrorops.MetaAnnotation
import mirrorops.Operation
import mirrorops.OpsMirror
import mirrorops.VoidType
import HttpService.Endpoints.Endpoint

import scala.NamedTuple.AnyNamedTuple
import scala.NamedTuple.NamedTuple
import scala.NamedTuple.Names

import quoted.*

trait HttpService[T]:
  val routes: Map[String, HttpService.Route]

object HttpService:
  import OpsMirror.*

  type Fields[N <: Tuple, O <: Tuple] =
    NamedTuple[N, Tuple.Map[O, OpToEndpoint]]

  type EncodeError[T] = T match
    case VoidType => Empty
    case _        => T

  type OperationInLabels[Ns <: Tuple] = Operation { type InputLabels = Ns }

  type OpToEndpoint[Op] = Op match
    case OperationIns[ins] =>
      Op match
        case OperationInLabels[inls] =>
          Op match
            case OperationOut[out] =>
              Op match
                case OperationErr[err] => Endpoint[NamedTuple[inls, ins], EncodeError[err], out]

  inline def derived[T](using m: OpsMirror.Of[T]): HttpService[T] = ${
    ServerMacros.derivedImpl[T]('m)
  }

  def endpoints[T: {HttpService as m, OpsMirror.Of as om}]: Endpoints[T] {
    type Fields = HttpService.Fields[om.MirroredOperationLabels, om.MirroredOperations]
  } =
    class EndpointsImpl[T, F <: AnyNamedTuple](val routes: Map[String, HttpService.Route]) extends Endpoints[T]:
      type Fields = F
      def selectDynamic(name: String): HttpService.Route = routes(name)
      def ++[U](that: Endpoints[U])(using Tuple.Disjoint[Names[Fields], Names[that.Fields]] =:= true): EndpointsImpl[T & U, NamedTuple.Concat[Fields, that.Fields]] =
        new EndpointsImpl[T & U, NamedTuple.Concat[Fields, that.Fields]](this.routes ++ that.routes)
    new EndpointsImpl[T, HttpService.Fields[om.MirroredOperationLabels, om.MirroredOperations]](m.routes)

  sealed trait Endpoints[T] extends Selectable:
    outer =>
    def routes: Map[String, HttpService.Route]
    type Fields <: AnyNamedTuple
    def selectDynamic(name: String): HttpService.Route
    def ++[U](that: Endpoints[U])(using Tuple.Disjoint[Names[Fields], Names[that.Fields]] =:= true): Endpoints[T & U] {
      type Fields = NamedTuple.Concat[outer.Fields, that.Fields]
    }


  object Endpoints:
    opaque type Endpoint[I, E, O] <: HttpService.Route = HttpService.Route

  sealed trait Empty

  object special:
    opaque type Static = Array[Byte]
    object Static:
      def apply(bytes: Array[Byte]): Static = bytes
      extension (static: Static)
        def render: Array[Byte] = static

  object model:
    class failsWith[E] extends ErrorAnnotation[E]

    enum method extends MetaAnnotation:
      case get(route: String)
      case post(route: String)
      case put(route: String)
      case delete(route: String)

    enum source extends MetaAnnotation:
      case path()
      case query()
      case body()
  end model

  case class Input(label: String, source: model.source)
  case class Route(route: model.method, inputs: Seq[Input])
end HttpService
