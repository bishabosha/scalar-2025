package ntdataframe

import java.time.LocalDate

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
      DataFrame.readCSV[Customer]("testResources/customers-100.csv")

    val na = df.columns[(firstname: ?, age: ?)]

    println(df.show())
    println(na.show())
    val bucketed = na.collectOn[(firstname: ?)].columns[(age: ?)]
    println(bucketed.keys.show())
    println(bucketed.get("jamie").get.show())

    val withDate = df.withComputed:
      (age_plus_10 = DataFrame.fun((age: Int) => age + 10)(col.age))

    println(withDate.show())

    val withToday = df.withValue((today = LocalDate.now()))

    println(withToday.show())

    val df1: DataFrame[Customer] =
      DataFrame.readCSV[Customer]("testResources/customers-200.csv")

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
