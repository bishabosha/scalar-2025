package ntquery

class NoteSuite extends munit.FunSuite:
  test("basic") {
    case object Note extends Table[(id: String, title: String, content: String)]

    println:
      Note.insert.values(
        (title = "Test Note", content = "This is a test note.")
      )
    println:
      Note.select
    println:
      Note.delete.filter(_.id === "1")
  }
