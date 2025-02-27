package ntdataframe

import scala.util.Using

import java.nio.file.{Files, Paths}
import ntdataframe.DataFrame.SparseArr

class DataFrameSuite extends munit.FunSuite:
  def readResource(path: String) =
    Files.readString(Paths.get(s"testResources/$path"))

  val exampleCSV1 = readResource("customers-100.csv")
  val exampleCSV2 = readResource("customers-200.csv")

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
