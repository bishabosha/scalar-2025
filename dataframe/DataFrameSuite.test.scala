package ntdataframe

import scala.util.Using

import java.nio.file.{Files, Paths}
import ntdataframe.DataFrame.SparseArr

class DataFrameSuite extends munit.FunSuite:
  def readTestFile(path: String) =
    Files.readString(Paths.get(s"testResources/$path"))

  lazy val exampleCSV1 = readTestFile("customers-100.csv")
  lazy val exampleCSV2 = readTestFile("customers-200.csv")

  lazy val exampleCSVa1 = readTestFile("customers-a.csv")
  lazy val exampleCSVa2 = readTestFile("customers-a2.csv")
  lazy val exampleCSVb1 = readTestFile("customers-b.csv")
  lazy val exampleCSVb2 = readTestFile("customers-b2.csv")
  lazy val exampleCSVc1 = readTestFile("customers-c.csv")
  lazy val exampleCSVc2 = readTestFile("customers-c2.csv")

  val exampleSparseArr = DataFrame.SparseArr(IArray(10, 20), IArray("abc", "def"))

  test("sparse array length"):
    assert(exampleSparseArr.size == 20)

  test("sparse array apply"):
    assert(exampleSparseArr(0) == "abc")
    assert(exampleSparseArr(9) == "abc")
    assert(exampleSparseArr(10) == "def")
    assert(exampleSparseArr(19) == "def")
    intercept[IndexOutOfBoundsException] {
      exampleSparseArr(-1)
    }
    intercept[IndexOutOfBoundsException] {
      exampleSparseArr(20)
    }

  test("sparse array slice single cell"):
    val sa = exampleSparseArr.slice(0, 1)
    assert(sa.size == 1, s"${sa.size} != 1")
    assert(sa(0) == "abc", sa)

  test("sparse array slice multi cell"):
    val sa = exampleSparseArr.slice(9, 11)
    assert(sa.size == 2, s"${sa.size} != 2")
    assert(sa(0) == "abc", sa)
    assert(sa(1) == "def", sa)
    val complex = SparseArr(IArray(5, 10, 15, 20), IArray("a", "b", "c", "d"))
    val cSlice = complex.slice(9, 16)
    assert(cSlice.size == 7)
    assert(cSlice(0) == "b")
    assert(cSlice(1) == "c")
    assert(cSlice(5) == "c")
    assert(cSlice(6) == "d")

  test("explore basic csv"):
    val df: DataFrame[Any] = DataFrame.readAnyCSV(exampleCSV1.linesIterator)
    assert(df.len == 5)
    val expectedShow =
      """shape: (5, 4)
        |┌─────┬───────────┬──────────┬─────┐
        |│ id  ┆ firstname ┆ lastname ┆ age │
        |╞═════╪═══════════╪══════════╪═════╡
        |│ abc ┆ fred      ┆ hampton  ┆ 23  │
        |│ def ┆ jamie     ┆ thompson ┆ 28  │
        |│ ovg ┆ jamie     ┆ xx       ┆ 57  │
        |│ ghi ┆ ada       ┆ lovelace ┆ 31  │
        |│ mno ┆ grace     ┆ kelly    ┆ 42  │
        |└─────┴───────────┴──────────┴─────┘""".stripMargin
    assert(df.show() == expectedShow, df.show())

  test("typed basic csv"):
    type Schema = (id: String, firstname: String, lastname: String, age: Int)
    val df: DataFrame[Schema] = DataFrame.readCSV[Schema](exampleCSV1.linesIterator)
    assert(df.len == 5)
    val expectedShow =
      """shape: (5, 4)
        |┌─────┬───────────┬──────────┬─────┐
        |│ id  ┆ firstname ┆ lastname ┆ age │
        |╞═════╪═══════════╪══════════╪═════╡
        |│ abc ┆ fred      ┆ hampton  ┆ 23  │
        |│ def ┆ jamie     ┆ thompson ┆ 28  │
        |│ ovg ┆ jamie     ┆ xx       ┆ 57  │
        |│ ghi ┆ ada       ┆ lovelace ┆ 31  │
        |│ mno ┆ grace     ┆ kelly    ┆ 42  │
        |└─────┴───────────┴──────────┴─────┘""".stripMargin
    assert(df.show() == expectedShow, df.show())

  test("typed filter columns csv"):
    type Schema = (firstname: String, lastname: String)
    val df: DataFrame[Schema] = DataFrame.readCSV[Schema](exampleCSV1.linesIterator)
    assert(df.len == 5)
    val expectedShow =
      """shape: (5, 2)
        |┌───────────┬──────────┐
        |│ firstname ┆ lastname │
        |╞═══════════╪══════════╡
        |│ fred      ┆ hampton  │
        |│ jamie     ┆ thompson │
        |│ jamie     ┆ xx       │
        |│ ada       ┆ lovelace │
        |│ grace     ┆ kelly    │
        |└───────────┴──────────┘""".stripMargin
    assert(df.show() == expectedShow, df.show())

  test("typed sparse column merge"):
    type Schema = (id: String, firstname: String, lastname: String)
    val df1: DataFrame[Schema] = DataFrame.readCSV[Schema](exampleCSVa1.linesIterator)
    val df2: DataFrame[Schema] = DataFrame.readCSV[Schema](exampleCSVa2.linesIterator)

    val teamA1 = df1.withValue((team = "A"))
    val teamA2 = df2.withValue((team = "A"))

    val teamA: DataFrame[(id: String, firstname: String, lastname: String, team: String)] =
      teamA1.merge(teamA2)
    val expectedShow =
      """shape: (2, 4)
        |┌─────┬───────────┬──────────┬──────┐
        |│ id  ┆ firstname ┆ lastname ┆ team │
        |╞═════╪═══════════╪══════════╪══════╡
        |│ abc ┆ fred      ┆ hampton  ┆ A    │
        |│ def ┆ jamie     ┆ thompson ┆ A    │
        |└─────┴───────────┴──────────┴──────┘""".stripMargin
    assert(teamA.show() == expectedShow, teamA.show())

  test("typed sparse column merge collectOn"):
    type Schema = (id: String, firstname: String, lastname: String)
    val dfa1: DataFrame[Schema] = DataFrame.readCSV[Schema](exampleCSVa1.linesIterator)
    val dfa2: DataFrame[Schema] = DataFrame.readCSV[Schema](exampleCSVa2.linesIterator)
    val dfb1: DataFrame[Schema] = DataFrame.readCSV[Schema](exampleCSVb1.linesIterator)
    val dfb2: DataFrame[Schema] = DataFrame.readCSV[Schema](exampleCSVb2.linesIterator)
    val dfc1: DataFrame[Schema] = DataFrame.readCSV[Schema](exampleCSVc1.linesIterator)
    val dfc2: DataFrame[Schema] = DataFrame.readCSV[Schema](exampleCSVc2.linesIterator)

    val teamA1 = dfa1.withValue((team = "A"))
    val teamA2 = dfa2.withValue((team = "A"))
    val teamB1 = dfb1.withValue((team = "B"))
    val teamB2 = dfb2.withValue((team = "B"))
    val teamC1 = dfc1.withValue((team = "C"))
    val teamC2 = dfc2.withValue((team = "C"))

    val outOfOrder: DataFrame[(id: String, firstname: String, lastname: String, team: String)] =
      teamA1.merge(teamB1).merge(teamC1).merge(teamA2).merge(teamB2).merge(teamC2)

    // TODO: Add another sparse column (partition - basically split the data into 2 halves),
    // such that the sparse column will be split by the ranges of the sparse team column.

    val expectedShow =
      """shape: (7, 4)
        |┌─────┬───────────┬──────────┬──────┐
        |│ id  ┆ firstname ┆ lastname ┆ team │
        |╞═════╪═══════════╪══════════╪══════╡
        |│ abc ┆ fred      ┆ hampton  ┆ A    │
        |│ ovg ┆ jamie     ┆ xx       ┆ B    │
        |│ ixq ┆ gemma     ┆ hampton  ┆ C    │
        |│ def ┆ jamie     ┆ thompson ┆ A    │
        |│ ghi ┆ ada       ┆ lovelace ┆ B    │
        |│ sed ┆ anna      ┆ medoc    ┆ C    │
        |│ fci ┆ robert    ┆ burns    ┆ C    │
        |└─────┴───────────┴──────────┴──────┘""".stripMargin
    assert(outOfOrder.show() == expectedShow, outOfOrder.show())

    val ordered = outOfOrder.collectOn[(team: ?)]
    val teamA = ordered.get("A").get
    val teamB = ordered.get("B").get
    val teamC = ordered.get("C").get

    assert(teamA.show() == teamA1.merge(teamA2).show(), teamA.show())
    assert(teamB.show() == teamB1.merge(teamB2).show(), teamB.show())
    assert(teamC.show() == teamC1.merge(teamC2).show(), teamC.show())
