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

case class Player(id: String,
                  placement: Double,
                  x: Double,
                  y: Double) {
  override def toString: String = {
    s"$id,$placement,$x,$y"
  }
}

object Player {
  def apply(s: Seq[String]): Player = {
    Player(s(0), s(1).toDouble, s(2).toDouble, s(3).toDouble)
  }
}