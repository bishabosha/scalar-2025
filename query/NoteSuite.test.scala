package ntquery

import ntquery.InsertQuery.InsertTo
import ntquery.Expr.Eq
import ntquery.Expr.Sel
import ntquery.Expr.Ref
import scala.collection.BuildFrom
import scala.collection.mutable
import scala.reflect.ClassTag
import scala.annotation.targetName

class NoteSuite extends munit.FunSuite:

  test("run query on in-memory store") {
    case object Note extends Table[(id: String, title: String, content: String)]

    trait Dao(db: DB):
      def createNote(title: String, content: String) =
        db.run(
          Note.insert.values(
            (title = title, content = content)
          )
        )
      def getAll() =
        db.run(
          Note.select
        )
      def deleteNoteById(id: String): Unit =
        db.run(
          Note.delete.filter(_.id === id)
        )

    val db = InMemoryStore()
    object NoteDao extends Dao(db)

    assert(db.allRecordCount == 0)
    val note1 = NoteDao.createNote(title = "test", content = "some content")

    assert(note1.id == "0")
    assert(note1.title == "test")
    assert(note1.content == "some content")

    assert(db.allRecordCount == 1)
    val fetch = db.getData(Note, note1.id)
    assert(fetch.isDefined)
    assert(fetch.get == Map("id" -> note1.id, "title" -> "test", "content" -> "some content"))
    val all: Seq[?] = NoteDao.getAll()
    assert(all == Seq(note1))
    assert(db.allRecordCount == 1)
    NoteDao.deleteNoteById(note1.id)
    assert(db.allRecordCount == 0)
  }
