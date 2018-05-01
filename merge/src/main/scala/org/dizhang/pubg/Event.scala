package org.dizhang.pubg



case class Event(weapon: String,
                 killer: Player,
                 map: String,
                 matchId: String,
                 time: Double,
                 victim: Player)
