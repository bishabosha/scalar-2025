package example

import example.model.Note
import ntquery.DB
import ntquery.LogBasedStore
import ntquery.Table
import serverlib.httpservice.HttpService
import serverlib.httpservice.HttpService.endpoints
import serverlib.httpservice.HttpService.model.method.get
import serverlib.httpservice.HttpService.model.source.path
import serverlib.httpservice.HttpService.special.Static
import serverlib.jdkhttp.Server.Ser
import serverlib.jdkhttp.Server.ServerBuilder
import serverlib.jdkhttp.Server.effestion
import serverlib.jdkhttp.upicklex.SerDes.given
import upicklex.namedTuples.Macros.Implicits.given


trait StaticService derives HttpService:
  @get("/")
  def index: Static

  @get("/assets/{rest}")
  def asset(@path rest: String): Static

case object Note extends Table[model.Note]

val store = LogBasedStore()

val schema = endpoints[StaticService] ++ endpoints[NoteService]

val app = effestion(schema)

def routes(db: DB): app.Routes = (
  index = _ => Static.fromResource("index.html"),
  asset = p => Static.fromResource(s"assets/${p.rest}"),
  createNote = p =>
    db.run:
      Note.insert.values(
        (title = p.body.title, content = p.body.content)
      )
  ,
  getAllNotes = _ =>
    db.run:
      Note.select
  ,
  deleteNote = p =>
    db.run:
      Note.delete.filter(_.id === p.id)
)

@main def serve =
  store.refreshTables(Note)
  val server = app
    .handle(routes(store))
    .listen(port = 8080)

  sys.addShutdownHook:
    server.close()
    store.flushLogToDisk()
