package example

import org.scalajs.dom.document
import scala.concurrent.ExecutionContext.Implicits.given
import model.Note

import com.raquo.laminar.api.L.*
import serverlib.fetchhttp.Client
import serverlib.fetchhttp.upicklex.SerDes.given
import serverlib.httpservice.HttpService
import ntdataframe.DataFrame
import ntdataframe.DataFrame.{fun, col, group}
import scala.deriving.Mirror
import ntdataframe.TupleUtils.{given Mirror}

import upicklex.namedTuples.Macros.Implicits.given
import ntdataframe.DataFrame.TagsOf
import ntdataframe.DFShow
import ntdataframe.laminar.LaminarDataFrame

val client = Client.ofEndpoints(
  HttpService.endpoints[NoteService],
  baseURI = "http://localhost:8080/"
)

def app: HtmlElement =
  val notesVar = Var(Seq.empty[Note])
  val analysisVar = Var(Option.empty[DataFrame[(word: String, freq: Int)]])
  val deleteBus = EventBus[(id: String)]()
  val saveClicks = EventBus[(title: String, content: String)]()
  val analyzeClicks = EventBus[String]()

  val deletionEvents = deleteBus.events
    .flatMapSwitch: form =>
      EventStream
        .fromFuture(client.deleteNote.send(form))
        .mapTo(form.id)

  val savedEvents = saveClicks.events
    .flatMapSwitch(form =>
      EventStream
        .fromFuture(
          client.createNote
            .send((body = form))
        )
    )

  val analysisEvents: EventStream[DataFrame[(word: String, freq: Int)]] =
    analyzeClicks.events
      .map: text =>
        def asWord(base: String) =
          val trimmed = base.toLowerCase.replaceAll("\\W", "")
          if trimmed.isEmpty then "<symbolic>" else trimmed

        DataFrame
          .column((base_word = text.split("\\s+")))
          .withComputed((word = fun(asWord)(col.base_word)))
          .groupBy(col.word)
          .agg(
            group.key ++ (freq = group.size)
          )
          .sort(col.freq, descending = true)

  def fetchNotesStream() =
    EventStream.fromFuture(
      client.getAllNotes.send(NamedTuple.Empty)
    )

  def noteElem(id: String, note: Signal[Note]) =
    div(
      className := "note",
      h2(text <-- note.map(_.title)),
      p(text <-- note.map(_.content)),
      button(
        "Delete Note",
        onClick.mapTo((id = id)) --> deleteBus.writer
      )
    )

  def form() =
    val titleInput = Var("")
    val contentTextArea = Var("")
    div(
      className := "note-form",
      input(
        placeholder := "Title",
        onChange.mapToValue --> titleInput.writer
      ),
      textArea(
        placeholder := "Content",
        onChange.mapToValue --> contentTextArea.writer
      ),
      button(
        "Create Note",
        onClick.compose(
          _.sample(titleInput.signal, contentTextArea.signal)
        ) --> saveClicks.writer
      ),
      button(
        "Analyse",
        onClick.compose(
          _.sample(contentTextArea.signal)
        ) --> analyzeClicks.writer
      )
    )
  end form

  def displayTable(df: DataFrame[(word: String, freq: Int)]) =
    div(
      className := "note-form",
      LaminarDataFrame.displayAnyTable(df),
      button(
        "Close Analysis",
        onClick.mapTo(None) --> analysisVar.writer
      )
    )

  div(
    idAttr := "app-container",
    h1("My Notepad"),
    form(),
    analysisEvents.map(Some(_)) --> analysisVar.writer,
    savedEvents --> notesVar.updater[Note](_ :+ _),
    deletionEvents --> notesVar.updater[String]((ns, id) =>
      ns.filterNot(_.id == id)
    ),
    child.maybe <-- analysisVar.signal.map(_.map(displayTable)),
    children <-- notesVar.signal.split(_.id)((id, _, sig) => noteElem(id, sig)),
    onMountBind(_ => fetchNotesStream() --> notesVar.writer)
  )
end app

@main def start: Unit =
  renderOnDomContentLoaded(document.body, app)
