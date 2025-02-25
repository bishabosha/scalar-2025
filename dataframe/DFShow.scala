package ntdataframe

import java.time.LocalDate

trait DFShow[T]:
  extension (t: T) def show: String

object DFShow:
  given DFShow[Int] with
    extension (t: Int) def show: String = t.toString
  given DFShow[Long] with
    extension (t: Long) def show: String = t.toString
  given DFShow[Double] with
    extension (t: Double) def show: String = t.toString
  given DFShow[String] with
    extension (t: String) def show: String = t
  given DFShow[Boolean] with
    extension (t: Boolean) def show: String = t.toString
  given DFShow[LocalDate] with
    extension (t: LocalDate) def show: String = t.toString
