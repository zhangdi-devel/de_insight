package org.dizhang.pubg

case class Match(date: Long,
                 gameSize: Int,
                 id: String,
                 mode: String,
                 partySize: Int) {
  override def toString: String = {
    s"$date,$gameSize,$partySize,$mode"
  }
}
