package ntdataframe

import scala.deriving.Mirror

class TupleUtilsSuite extends munit.FunSuite:

  test("basic mirror named tuple type"):
    assert(compiletime.testing.typeChecks("""
      import TupleUtils.given
      summon[Mirror.Of[(name: String, age: Int)]]"""))
    assert(!compiletime.testing.typeChecks("""
      summon[Mirror.Of[(name: String, age: Int)]]"""))

  test("mirror ops"):
    import TupleUtils.given

    case class Person(name: String, age: Int)

    val m = summon[Mirror.Of[(name: String, age: Int)]]

    val p: (name: String, age: Int) = m.fromProduct(Person("Alice", 30))
    assert(p.name == "Alice")
    assert(p.age == 30)
