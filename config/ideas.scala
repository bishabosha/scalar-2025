import NamedTuple.Empty

object Config:
  def apply[T]: Any = ???

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
