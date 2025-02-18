package example

import upickle.default.*
import serverlib.fetchhttp.PartialRequest.Des
import serverlib.fetchhttp.PartialRequest.Ser
import scala.util.NotGiven

object utils:
  // todo: move to another library

  given [T: Reader] => NotGiven[T =:= String] => Des[T]:
    def deserialize(s: String): T = read[T](s)

  given [T: Writer]  => NotGiven[T =:= String] => Ser[T]:
    def serialize(i: T): String = write(i)
