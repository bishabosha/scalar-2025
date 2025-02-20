package example

import serverlib.httpservice.HttpService
import serverlib.httpservice.HttpService.special.Static
import serverlib.httpservice.HttpService.model.method.get
import serverlib.httpservice.HttpService.model.source.path
import serverlib.jdkhttp.Server.ServerBuilder
import serverlib.jdkhttp.Server.Ser
import example.model.Note

import utils.{fromResource, given}
import upicklex.namedTuples.Macros.Implicits.given
import ntquery.DB
import ntquery.Table
import ntquery.InMemoryStore

trait StaticService derives HttpService:
  @get("/")
  def index: Static

  @get("/assets/{rest}")
  def asset(@path rest: String): Static

val e = HttpService.endpoints[NoteService] ++ HttpService.endpoints[StaticService]

case object Note extends Table[model.Note]

trait Dao(db: DB):
  def createNote(title: String, content: String): model.Note =
    db.run(
      Note.insert.values(
        (title = title, content = content)
      )
    )
  def getAll(): Seq[model.Note] =
    db.run(
      Note.select
    )
  def deleteNoteById(id: String): Unit =
    db.run(
      Note.delete.filter(_.id === id)
    )

object NoteDao extends Dao(InMemoryStore())

@main def serve =
  val server = ServerBuilder()
    .addEndpoints(e):
      (
        createNote = p => NoteDao.createNote(p.body.title, p.body.content),
        getAllNotes = _ => NoteDao.getAll(),
        deleteNote = p => NoteDao.deleteNoteById(p.id),
        index = _ => Static.fromResource("index.html"),
        asset = p => Static.fromResource(s"assets/${p.rest}")
      )
    .create(port = 8080)

  sys.addShutdownHook(server.close())
