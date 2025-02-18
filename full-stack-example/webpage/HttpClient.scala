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
import serverlib.fetchhttp.Client

import utils.given

class HttpClient(using ExecutionContext):
  // private val e = HttpService.endpoints[NoteService]
  private val client = Client.ofEndpoints(
    HttpService.endpoints[NoteService],
    baseURI = "http://localhost:8080/"
  )

  def getAllNotes(): Future[Seq[Note]] =
    client.getAllNotes.send(NamedTuple.Empty)

  def createNote(title: String, content: String): Future[Note] =
    client.createNote
      .send((body = (title = title, content = content)))

  def deleteNote(id: String): Future[Boolean] =
    client.deleteNote
      .send((id = id))
      .map(_ => true)
      .recoverWith(_ => Future.successful(false))
