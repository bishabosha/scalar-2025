package ntdataframe

import DataFrame.{col, fun, group}
import ntdataframe.DataFrame.Expr

@main def textstats: Unit =
  val toLower = (_: String).toLowerCase
  val text = "The quick brown fox jumps over the lazy dog"
  val stats: DataFrame[(case_insensitive: String, freq: Int)] =
    DataFrame
      .column((words = text.split("\\s+")))
      .withComputed(
        (case_insensitive = fun(toLower)(col.words))
      )
      .groupBy(col.case_insensitive)
      .agg(
        group.key ++ (freq = group.size)
      )
  val sorted = stats.sort(col.freq, descending = true)
  println(sorted.show(Int.MaxValue))
