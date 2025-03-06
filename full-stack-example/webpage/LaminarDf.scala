package ntdataframe.laminar

import ntdataframe.DataFrame.TagsOf
import scala.deriving.Mirror
import ntdataframe.DataFrame

import com.raquo.laminar.api.L.*
import ntdataframe.DFShow

object LaminarDataFrame:
  def displayAnyTable[T: {TagsOf as tags, Mirror.ProductOf}](df: DataFrame[T]) =
    table(
      className := "dataframe",
      thead(
        tr(
          tags.names.map(th(_)).toSeq
        )
      ),
      tbody(
        df.materialiseRows
          .map: row =>
            tr(
              tags.shows.iterator.zip(row.asInstanceOf[Product].productIterator)
                .map: (show, value) =>
                  td(show.asInstanceOf[DFShow[Any]].show(value))
                .toSeq
            )
          .toSeq
      )
    )
