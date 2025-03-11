package ntforeign

import scala.NamedTuple
import scala.annotation.experimental
import scala.util.Using
import java.lang.foreign.Arena

@experimental
object StructExample:
  import OffHeapStruct.*

  def main(args: Array[String]): Unit = Using(Arena.ofConfined()) { arena =>
    given Arena = arena

    // Define a Person struct with name, age, and salary
    type PersonStruct = (x: Byte, y: Int, width: Byte, height: Int)

    // Allocate a new struct
    val rect = allocate[PersonStruct]

    // Initialize with values
    rect.update((x = 10.toByte, y = 20, width = 100.toByte, height = 50))

    // Access individual fields
    println(
      s"Rectangle: x=${rect("x")}, y=${rect("y")}, width=${rect("width")}, height=${rect("height")}"
    )

    // Modify a single field
    rect.update("width", 120.toByte)

    // Get the full struct as a named tuple
    val updatedRect = rect.get
    println(
      s"Updated rectangle: x=${updatedRect.x}, y=${updatedRect.y}, " +
        s"width=${updatedRect.width}, height=${updatedRect.height}"
    )

    // Example of a more complex struct
    val point3d = allocateWithValues((x = 1.0f, y = 2.0f, z = 3.0f, isVisible = true))
    println(s"3D Point: ${point3d.get}")

    // Example of a Vector struct
    type Vector = (x: Float, y: Float, z: Float, w: Float)

    // Allocate an array of vectors
    val vectorArray = new Array[Struct[Vector]](10)
    for (i <- 0 until 10) {
      vectorArray(i) = allocateWithValues((x = i.toFloat, y = i * 2.0f, z = i * 3.0f, w = 1.0f))
    }

    // Print out vector array
    vectorArray.foreach(v => println(s"Vector: ${v.get}"))
  }.get
end StructExample
