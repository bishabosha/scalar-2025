package issub

import substructural.Sub.Substructural

def test2 =
  summon[(x: Int) =:= (x: Int)]
  summon[(x: Int) <:< (x: Int)]
  // summon[(x: Int) <:< (x: Int, y: Int)]
  summon[Substructural[(x: (y: Int)), (x: (y: Int), y: Int)]]

val y1: Substructural[(foo: (bar: 1)), (foo: (bar: Int, qux: String), other: Boolean)] = Substructural.proven