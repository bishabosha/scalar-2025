package upicklex

import upickle.default.*
import upicklex.namedTuples.Macros.Implicits.given

@main def demo =
  println(
    write(
      (
        (x = 23, y = 7, z = 50),
        (name = "Alice", age = 28),
        (arr = (1, 2, 3))
      )
    )
  )
  val foo = read[(arr: Seq[Int], origin: (x: Int, y: Int))](
    """{"arr":[1,2,3], "origin":{"x":23,"y":7}}"""
  )
  println(foo.arr)
  println(foo.origin)
