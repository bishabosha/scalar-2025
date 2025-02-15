package example

import upickle.default.*
import serverlib.fetchhttp.PartialRequest.Des
import serverlib.fetchhttp.PartialRequest.Ser

object utils:
  // todo: move to another library

  given [T: Reader]: Des[T] = new Des[T]:
    def deserialize(s: String): T = read[T](s)

  given [T: Writer]: Ser[T] = new Ser[T]:
    def serialize(i: T): String = write(i)
