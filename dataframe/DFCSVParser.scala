package ntdataframe

import java.time.LocalDate

import NamedTuple.AnyNamedTuple
import NamedTuple.DropNames
import NamedTuple.NamedTuple

trait ParsersOf[T <: AnyNamedTuple]:
  def parsers: IArray[DFCSVParser[?]]

object ParsersOf:
  final class ParsersOfNT[T <: AnyNamedTuple](ps: Tuple) extends ParsersOf[T]:
    val parsers = ps.toIArray.map(_.asInstanceOf[DFCSVParser[?]])

  transparent inline given [T <: AnyNamedTuple]: ParsersOf[T] =
    new ParsersOfNT[T](compiletime.summonAll[Tuple.Map[DropNames[T], DFCSVParser]])

trait DFCSVParser[T]:
  def parse(cell: String): T

object DFCSVParser:
  given DFCSVParser[Int] with
    def parse(cell: String): Int = cell.toInt
  given DFCSVParser[Long] with
    def parse(cell: String): Long = cell.toLong
  given DFCSVParser[Double] with
    def parse(cell: String): Double = cell.toDouble
  given DFCSVParser[String] with
    def parse(cell: String): String = cell
  given DFCSVParser[Boolean] with
    def parse(cell: String): Boolean = cell.toBoolean
  given DFCSVParser[LocalDate] with
    def parse(cell: String): LocalDate = LocalDate.parse(cell)
