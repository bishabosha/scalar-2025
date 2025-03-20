package ntdataframe

import DataFrame.{col, fun, group}
import ntdataframe.DataFrame.Expr

@main def textstats: Unit =
  val toLower = (_: String).toLowerCase
  val text = "The quick brown fox jumps over the lazy dog"
  val stats = DataFrame
    .column((words = text.split("\\s+")))
    .withComputed(
      (lowerCase = fun(toLower)(col.words))
    )
    .groupBy(col.lowerCase)
    .agg(
      group.key ++ (freq = group.size)
    )
    .sort(col.freq, descending = true)
  println(stats.show(Int.MaxValue))
