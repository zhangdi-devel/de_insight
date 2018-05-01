package org.dizhang.pubg



case class Event(weapon: String,
                 killer: Player,
                 map: String,
                 matchId: String,
                 time: Double,
                 victim: Player) {
  override def toString: String = {
    s"$matchId,$map,$killer,$victim,$weapon,$time"
  }
}
