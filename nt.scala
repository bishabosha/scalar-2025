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
2. typed schema derivation from naked named-tuples
 */

val a =
  (
    foo = ("bar", "baz"),
    bar = (
      baz = 42,
      qux = ("quux", "quuz")
    )
  )
