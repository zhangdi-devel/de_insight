/*
 *    Copyright 2018 Zhang Di
 *
 *    Licensed under the Apache License, Version 2.0 (the "License");
 *    you may not use this file except in compliance with the License.
 *    You may obtain a copy of the License at
 *
 *        http://www.apache.org/licenses/LICENSE-2.0
 *
 *    Unless required by applicable law or agreed to in writing, software
 *    distributed under the License is distributed on an "AS IS" BASIS,
 *    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *    See the License for the specific language governing permissions and
 *    limitations under the License.
 */

package org.dizhang.pubg

case class Game(date: Long,
                gameSize: Int,
                id: String,
                mode: String,
                partySize: Int) {
  override def toString: String = {
    s"$date,$gameSize,$partySize,$mode"
  }
}

object Game {
  def apply(data: Seq[String]): Game = {
    Game(data(0).toLong, data(1).toInt, "", data(3), data(2).toInt)
  }
}