package ntdataframe

import java.time.LocalDate

object demo {
  type Session = (duration: Long, pulse: Long, maxPulse: Long, calories: Double)

  val df: DataFrame[Session] =
    DataFrame.fromCSV[Session]("data.csv")

  val d: DataFrame[(duration: Long)] =
    df.columns[(duration: ?)]

  val pm: DataFrame[(pulse: Long, maxPulse: Long)] =
    df.columns[(pulse: ?, maxPulse: ?)]
}

object customers {
  import DataFrame.col

  type Customer = (
      id: String,
      firstname: String,
      lastname: String,
      age: Int
  )

  @main def readcustomers(): Unit =
    val df: DataFrame[Customer] =
      DataFrame.fromCSV[Customer]("customers-100.csv")

    val na = df.columns[(firstname: ?, age: ?)]

    println(df.show())
    println(na.show())
    val bucketed = na.collectOn[(firstname: ?)].columns[(age: ?)]
    println(bucketed.keys.show())
    println(bucketed.get("jamie").get.show())

    val withDate = df.withComputed:
      (age_plus_10 = DataFrame.fun((age: Int) => age + 10)(Tuple(col.age)))

    println(withDate.show())

    val withToday = df.withValue((today = LocalDate.now()))

    println(withToday.show())

    val df1: DataFrame[Customer] =
      DataFrame.fromCSV[Customer]("customers-200.csv")

    val merged = df.merge(df1)
    println(merged.show())

    val byLast = merged.collectOn[(lastname: ?)].columns[(id: ?, age: ?)]
    println(byLast.keys.show())
    println(byLast.get("hampton").get.show())

    val byAge = merged.collectOn[(age: ?)].columns[(age: ?, firstname: ?, lastname: ?)]
    println(byAge.keys.show())
    println(byAge.get(31).get.show())

    val reversedColumns = merged.columns[(age: ?, lastname: ?, firstname: ?, id: ?)]
    println(reversedColumns.show(n = 1))
}

// TODO: expression with aggregations? look at pola.rs and pokemon dataset

object bankaccs {
  import DataFrame.col

  type Hsbc =
    (id: String, date: LocalDate, kind: String, merchant: String, diff: BigDecimal)
  type Monzo =
    (id: String, date: LocalDate, category: String, kind: String, name: String, diff: BigDecimal)
  type CreditSuisse =
    (id: String, date: LocalDate, category: String, merchant: String, diff: BigDecimal)

  val hsbc: DataFrame[Hsbc] = ???
  val monzo: DataFrame[Monzo] = ???
  val creditsuisse: DataFrame[CreditSuisse] = ???

  type Cols = (acc_kind: ?, id: ?, cat_computed: ?)

  def hsbcCat(kind: String, merchant: String): String = ???
  def monzoCat(category: String, kind: String, name: String): String = ???
  def creditsuisseCat(category: String, merchant: String): String = ???

  val all =
    hsbc
      .withValue((acc_kind = "hsbc"))
      .withComputed:
        (cat_computed = DataFrame.fun(hsbcCat)((col.kind, col.merchant)))
      .columns[Cols]
      .merge:
        monzo
          .withValue((acc_kind = "monzo"))
          .withComputed:
            (cat_computed = DataFrame.fun(monzoCat)((col.category, col.kind, col.name)))
          .columns[Cols]
      .merge:
        creditsuisse
          .withValue((acc_kind = "creditsuisse"))
          .withComputed:
            (cat_computed = DataFrame.fun(creditsuisseCat)((col.category, col.merchant)))
          .columns[Cols]

  val byKind = all.collectOn[(acc_kind: ?)].columns[(id: ?, cat_computed: ?)]
  val kinds = byKind.keys
  val hsbcAgg = byKind.get("hsbc").get
  val monzoAgg = byKind.get("monzo").get
  val creditsuisseAgg = byKind.get("creditsuisse").get
}
