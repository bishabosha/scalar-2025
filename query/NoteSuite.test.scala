package ntquery

class NoteSuite extends munit.FunSuite:
  test("basic") {
    object Note extends Table[(id: String, title: String, content: String)]

    Note.insert // todo some way to select specific fields?
    Note.select // just get all
    Note.delete.filter(_.id === "1")
  }
