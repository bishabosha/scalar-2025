package example

import org.scalajs.dom.*
import scala.scalajs.js

import java.io.IOException

import scala.concurrent.Future
import scala.concurrent.ExecutionContext

import upickle.default.*
import upicklex.namedTuples.Macros.Implicits.given
import example.model.Note
import serverlib.httpservice.HttpService.Endpoints
import serverlib.httpservice.HttpService
import serverlib.fetchhttp.PartialRequest
import serverlib.fetchhttp.PartialRequest.Des

import utils.given

class HttpClient(using ExecutionContext):
  private val e = HttpService.endpoints[NoteService]

  def getAllNotes(): Future[Seq[Note]] =
    PartialRequest(e.getAllNotes, "http://localhost:8080")
      .prepare(NamedTuple.Empty)
      .send()

  def createNote(title: String, content: String): Future[Note] =
    PartialRequest(e.createNote, "http://localhost:8080")
      .prepare((body = (title = title, content = content)))
      .send()

  def deleteNote(id: String): Future[Boolean] =
    PartialRequest(e.deleteNote, "http://localhost:8080")
      .prepare((id = id))
      .send()
      .map(_ => true)
      .recoverWith(_ => Future.successful(false))
