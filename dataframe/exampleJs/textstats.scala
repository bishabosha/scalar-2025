package ntdataframe

import DataFrame.{col, fun, group}
import ntdataframe.DataFrame.Expr

val toLower = (_: String).toLowerCase

@main def textstats: Unit =
  val text = "The quick brown fox jumps over the lazy dog"
  val df = DataFrame.column((words = text.split("\\s+")))
  println(df.show(Int.MaxValue))

  val composed: DataFrame[(words: String, case_insensitive: String)] = df
    .withComputed(
      (case_insensitive = fun(toLower)(col.words))
    )

  println(composed.show(Int.MaxValue))

  val stats: DataFrame[(case_insensitive: String, freq: Int)] = composed
    .groupBy[(case_insensitive: ?)]
    .agg(
      group.key ++ (freq = group.size)
    )

  println(stats.show(Int.MaxValue))
