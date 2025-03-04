package ntdataframe

import DataFrame.{col, fun}
import ntdataframe.DataFrame.Expr

object group:
  def key = (id = "id") // proxy for the colum that is grouped on
  def size = ???

val toLower = (_: String).toLowerCase

@main def textstats: Unit =
  val text = "The quick brown fox jumps over the lazy dog"
  val df = DataFrame.column((words = text.split("\\s+")))
  println(df.show(Int.MaxValue))
  val agg = df.withComputed(
    (case_insensitive = fun(toLower)(col.words))
  )
  println(agg.show(Int.MaxValue))
  println(agg.groupBy[(case_insensitive: ?)].keys.show())

  val stats = agg.groupBy[(case_insensitive: ?)].agg(group.key ++ (freq = group.size))
