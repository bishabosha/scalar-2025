/* === Thesis ===
Scala 3.6 stabilises the Named Tuples proposal in the main language.
It gives us new syntax for structural types and values,
and tools for programmatic manipulation of structural types without macros.
Can we, and should we, push it to the limit? of course!
let's explore DSL's for config, data, and scripting, for a more dynamic feel.
 */

/* === Plan ===
1. show new syntax for named tuples
  a. type syntax
  b. value syntax
2. show new Selectable Fields type member.
3. DSL for scripting
  a. advent of code problem
  b. generate invoice
  c. codegen
4. DSL for config
  a. static website generator
5. DSL for data
  a. json flavors
  b. data-frame
  c. sql query
  d. data-oriented programming
    aa. array of structs (on/off heap)
    bb. struct of arrays (on/off heap)
7. DSL for web servers
  a. client-call/server-logic for endpoint
 */

/* === Research work ===
1. revised ops-mirror based on named tuples
2. encoding of trees in an array?
3. basically data frame - SArray[(x: Int, y: Int)] => (Array[Int], Array[Int])
3. off-heap structs?
4. typed schema derivation from naked named-tuples
5. can I hava a basic json dsl for use with sttp?
  a. with extraction of fields? (like a cursor)
  b. pattern matching?
  c. can I implement a simple encoder/decoder?
6. can I get json working with ops-mirror?
 */

// type NamedTuple[N <: Tuple, T <: Tuple]
// import scala.language.strictEquality
import scala.NamedTuple.NamedTuple
import scala.NamedTuple.AnyNamedTuple
import scala.NamedTuple.Empty
import stringmatching.regex.Interpolators.r

class Config[T <: AnyNamedTuple]

// TODO: turn this into a Config[T] type (so JSON like DSL that can be parsed to data)
val config = (
  cls = (
    name = "ModuleKind",
    sum = true,
    modifier = "private",
    ctor = List(
      "NoModule" -> "NoModule.type",
      "CommonJSModule" -> "CommonJSModule.type",
      "ESModule" -> "ESModule.type"
    ),
    values = Nil
  ),
  padding = 4
)
// derived values

val configX = Config[
  (
      cls: (
          name: "ModuleKind",
          sum: true,
          modifier: "private",
          ctor: (
              NoModule: "NoModule.type",
              CommonJSModule: "CommonJSModule.type",
              ESModule: "ESModule.type"
          ),
          values: Empty
      ),
      padding: 4
  )
]

val a =
  (
    foo = ("bar", "baz"),
    bar = (
      baz = 42,
      qux = ("quux", "quuz")
    )
  )

import NamedTuple.withNames
import scala.deriving.Mirror

// type Person = (name: String, age: Int)
// val p: Person = ("Alice", 42).withNames[("name", "age")]
// assert(p(1) == p.age)
// summon[Mirror.Of[Person]].fromProduct(p.toTuple)

case class City(name: String, population: Int)

val Warsaw: NamedTuple.From[City] = (name = "Warsaw", population = 1_800_000)

class Data[Schema] extends scala.Selectable:
  type Fields = NamedTuple.Map[NamedTuple.From[Schema], Data]
  def selectDynamic(name: String): Data[?] = ???
object Data:
  def apply[Schema](s: Schema): Data[Schema] = ???

val p: Data[(name: String, age: Int)] =
  Data((name = "Alice", age = 42))
val name: Data[String] = p.name
val age: Data[Int] = p.age

case class City(name: String, population: Int, ...)
val c: Expr[City] = ??? // provided by a query
val name: Expr[String] = c.name
val pop: Expr[Int] = c.population
