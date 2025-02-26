package ntdataframe

import scala.util.Using

import java.nio.file.{Files, Paths}

class DataFrameSuite extends munit.FunSuite:
  def readResource(path: String) =
    Files.readString(Paths.get(s"testResources/$path"))

  val exampleCSV1 = readResource("customers-100.csv")
  val exampleCSV2 = readResource("customers-200.csv")

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
