package serverlib.jdkhttp.upicklex

import upickle.default.*
import serverlib.jdkhttp.Server.Ser
import serverlib.httpservice.HttpService.special.Static
import serverlib.jdkhttp.Server.Des
import java.nio.charset.StandardCharsets
import scala.util.NotGiven

object SerDes:
  given [T: Reader] => NotGiven[T =:= String] => Des[T]:
    def deserialize(s: String): T = read[T](s)

  given [T: Writer] => NotGiven[T =:= String] => Ser[T]:
    def serialize(i: T) = Right(
      Some("application/json; charset=utf-8" -> write(i).getBytes(StandardCharsets.UTF_8))
    )
