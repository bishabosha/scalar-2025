//> using toolkit 0.7.0
import sttp.client4.quick.*
import upickle.default.*

import upicklex.namedTuples.Macros.Implicits.given

@main def ollama(model: String, query: String) =
  val r = quickRequest
    .post(uri"http://localhost:11434/api/chat")
    .body(
      write(
        (
          model = model,
          messages = Seq(
            (
              role = "user",
              content = query
            )
          ),
          stream = false
        )
      )
    )
    .send()

  val msg = read[(message: (content: String))](r.body)

  println(msg.message.content)
