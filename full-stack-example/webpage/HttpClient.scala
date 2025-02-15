package example

import org.scalajs.dom.*
import scala.scalajs.js

import java.io.IOException

import scala.concurrent.Future
import scala.concurrent.ExecutionContext

import upickle.default.*
import upicklex.namedTuples.Macros.Implicits.given
import example.model.Note

class HttpClient(using ExecutionContext):

  def getAllNotes(): Future[Seq[Note]] =
    for
      resp <- Fetch.fetch("./api/notes").toFuture
      notes <- resp.to[Seq[Note]]
    yield notes

  def createNote(title: String, content: String): Future[Note] =
    val request = Request(
      "./api/notes",
      new:
        method = HttpMethod.POST
        headers = js.Dictionary("Content-Type" -> "application/json")
        body = write((title = title, content = content))
    )
    for
      resp <- Fetch.fetch(request).toFuture
      note <- resp.to[Note]
    yield note

  def deleteNote(id: String): Future[Boolean] =
    val request = Request(
      s"./api/notes/$id",
      new:
        method = HttpMethod.DELETE
    )
    for
      resp <- Fetch.fetch(request).toFuture
      res <- resp.to[Boolean]
    yield res

  extension (resp: Response)
    private def to[T: Reader]: Future[T] =
      if resp.ok then
        for json <- resp.text().toFuture
        yield read[T](json)
      else Future.failed(new IOException(resp.statusText))
