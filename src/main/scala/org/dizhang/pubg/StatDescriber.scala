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
/**
  * Parse from event
  * and map counter names to indices
  * */
sealed trait StatDescriber[A] extends Serializable {

  def size: Int

  def fromEvent(elem: A): List[(String, Long, Array[Int])]

  def names: Array[String]

}

object StatDescriber {


  class Cnt2[A](namePair: (String, String),
                keyFun: A => (String, String),
                timeFun: A => Long) extends StatDescriber[A] {
    def size = 2

    val names = Array(namePair._1, namePair._2)

    override def fromEvent(elem: A): List[(String, Long, Array[Int])] = {
      List(
        (keyFun(elem)._1, timeFun(elem), Array(1,0)),
        (keyFun(elem)._2, timeFun(elem), Array(0,1))
      )
    }

  }

  /**
  class SimpleGrade extends StatDescriber[(Record, Long)] {

    def size = 2

    override def fromEvent(elem: (Record, Long)): List[(String, Long, Array[Int])] = {
      val event =  elem._1.event
      val time = elem._2
      List(
        (event.killer.id, time, Array(1,0)),
        (event.victim.id, time, Array(0,1))
      )
    }

    override def names: Array[String] = Array("kills", "deaths")

  }

  class SimpleCredit extends StatDescriber[Report] {

    def size = 2

    override def fromEvent(elem: Report): List[(String, Long, Array[Int])] = {
      List(
        (elem.reporter, elem.time, Array(1,0)),
        (elem.cheater, elem.time, Array(0,1))
      )
    }

    override def names: Array[String] = Array("reports", "reported")
  }
  */
}
