package example

import org.scalajs.dom.document
import scala.concurrent.ExecutionContext.Implicits.given
import model.Note

import com.raquo.laminar.api.L.*
import serverlib.fetchhttp.Client
import serverlib.httpservice.HttpService

import upicklex.namedTuples.Macros.Implicits.given
import utils.given

val client = Client.ofEndpoints(
  HttpService.endpoints[NoteService],
  baseURI = "http://localhost:8080/"
)

def app: HtmlElement =
  val notesVar = Var(Seq.empty[Note])
  val deleteBus = EventBus[(id: String)]()
  val saveClicks = EventBus[(title: String, content: String)]()

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

  def noteElem(note: Note) =
    div(
      className := "note",
      h2(note.title),
      p(note.content),
      button(
        "Delete Note",
        onClick.mapTo((id = note.id)) --> deleteBus.writer
      ),
      deletionEvents --> notesVar.updater[String]((ns, id) =>
        ns.filterNot(_.id == id)
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
        onClick.mapToUnit.compose(
          _.sample(titleInput.signal, contentTextArea.signal)
        ) --> saveClicks.writer
      ),
      savedEvents --> notesVar.updater[Note](_ :+ _)
    )
  end form

  div(
    idAttr := "app-container",
    h1("My Notepad"),
    form(),
    children <-- notesVar.signal.map(_.map(noteElem)),
    onMountBind(_ =>
      EventStream.fromFuture(
        client.getAllNotes.send(NamedTuple.Empty)
      ) --> notesVar.writer
    )
  )
end app

@main def start: Unit =
  renderOnDomContentLoaded(document.body, app)
