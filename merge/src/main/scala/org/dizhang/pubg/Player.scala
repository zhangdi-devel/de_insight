package org.dizhang.pubg

case class Player(id: String,
                  placement: Double,
                  x: Double,
                  y: Double)

object Player {
  def apply(s: Seq[String]): Player = {
    Player(s(0), s(1).toDouble, s(2).toDouble, s(3).toDouble)
  }
}