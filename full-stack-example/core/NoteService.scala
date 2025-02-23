package example

import serverlib.httpservice.HttpService
import HttpService.model.*, source.*, method.*

import model.Note

// @failsWith[Unit]
trait NoteService derives HttpService:
  @post("/api/notes")
  def createNote(@body body: (title: String, content: String)): Note

  @get("/api/notes")
  def getAllNotes(): Seq[Note]

  @delete("/api/notes/{id}")
  def deleteNote(@path id: String): Unit
