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

trait StaticService derives HttpService:
  @get("/")
  def index: Static

  @get("/assets/{rest}")
  def asset(@path rest: String): Static

val e = HttpService.endpoints[NoteService] ++ HttpService.endpoints[StaticService]

val notes: collection.mutable.Map[String, Note] = collection.mutable.Map.empty
def id() = java.util.UUID.randomUUID().toString

@main def serve =
  val server = ServerBuilder()
    .addEndpoints(e):
      (
        createNote = p =>
          val n = (id = id(), title = p.body.title, content = p.body.content)
          notes(n.id) = n
          n
        ,
        getAllNotes = _ => notes.values.toList,
        deleteNote = p =>
          if notes.contains(p.id) then notes -= p.id
          else throw new Exception(s"Note not found with id `${p.id}`"),
        index = _ => Static.fromResource("index.html"),
        asset = p => Static.fromResource(s"assets/${p.rest}")
      )
    .create(port = 8080)

  sys.addShutdownHook(server.close())
