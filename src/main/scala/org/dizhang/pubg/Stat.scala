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

sealed trait Stat {
  def zero: Array[Int]
  def add(x: Array[Int], y: Array[Int]): Array[Int]
  def merge(x: Array[Int], y: Array[Int]): Array[Int]
  def sub(x: Array[Int], y: Array[Int]): Array[Int]
}

object Stat {


  type KeyedCounter = (String, Long, Array[Int])

  class Counter(val length: Int) extends Stat {

    val zero: Array[Int] =  Array.fill(length)(0)

    override def add(x: Array[Int], y: Array[Int]): Array[Int] = {
      x.zip(y).map(p => p._1 + p._2)
    }

    override def sub(x: Array[Int], y: Array[Int]): Array[Int] = {
      x.zip(y).map(p => p._1 - p._2)
    }

    override def merge(x: Array[Int], y: Array[Int]): Array[Int] = {
      x ++ y
    }

  }


  case class Grade(kills: Int, deaths: Int) {
    def ++(that: Grade): Grade = {
      Grade(this.kills + that.kills, this.deaths + that.deaths)
    }
  }

  case class Credit(report: Int, reported: Int)

  case class KeyedGrade(player: String, grade: Grade, time: Long)

  case class KeyedCredit(player: String, credit: Credit, time: Long)

  case class Merged(player: String, grade: Grade, credit: Credit)

}
