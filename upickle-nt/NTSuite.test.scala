package upicklex.namedTuples

import Macros.Implicits.given
import upickle.default.*

class NTSuite extends munit.FunSuite {
  test("write nt as plain json") {
    assert(write((name = "Jamie", age = 28)) == """{"name":"Jamie","age":28}""")
  }
  test("read plain json as nt") {
    assert(
      read[(name: String, age: Int)]("""{"name":"Jamie","age":28}""")
        == (name = "Jamie", age = 28)
    )
  }
}
