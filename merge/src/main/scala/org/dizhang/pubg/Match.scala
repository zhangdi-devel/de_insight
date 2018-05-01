package org.dizhang.pubg

import java.util.Date

case class Match(date: Date,
                 gameSize: Int,
                 id: String,
                 mode: String,
                 partySize: Int)
