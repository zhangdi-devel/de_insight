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
import PlayerStatesSpec._

class PlayerStatesSpec extends BaseSpec {

  def getPS(windows: Int, windowSize: Int) =
    new PlayerStates(windows, windowSize, 2, 2)

  def elems(start: Int, end: Int): Iterator[(Array[Int], Long, Boolean)] = {
    (start until end).map{i =>
      val cnt = if (i%2 == 0) Array(1,0) else Array(0,1)
      val time = (i * 1000).toLong
      val first = i%9 != 0
      (cnt, time, first)
    }.toIterator
  }

  "A PlayerStates" should "handle consecutive elements" in {
    val ps = getPS(60, 60)
    elems(0, 7200).foreach { r =>
      //ps.addElement(r._1, r._2, r._3)
    }
    logger.info(ps.pprint())
  }

  ignore should "handle jumping elements" in {
    val ps = getPS(5, 60)
    elems(0, 300).foreach(r => ps.addElement(r._1, r._2, r._3))
    logger.info(ps.pprint())
    elems(800, 860).foreach{r =>
      ps.addElement(r._1, r._2, r._3)
    }
    logger.info(ps.pprint())

    elems(1000, 1120).foreach(r => ps.addElement(r._1, r._2, r._3))
    logger.info(ps.pprint())

  }

  ignore should "handle month" in {
    val ps = getPS(30, 86400)
    ps.addElement(Array(0,1), 1483232307573L, false)
    logger.info(ps.pprint())
    ps.addElement(Array(0,1), 1483232369018L, false)
    logger.info(ps.pprint())
    ps.addElement(Array(0,1), 1483233246310L, false)
    logger.info(ps.pprint())
  }

  ignore should "handle hours" in {
    val ps = getPS(60, 60)
    addRpted(ps, 1483232307573L)
    addRpted(ps, 1483232369018L)
    addRpted(ps, 1483233246310L)
    addKill(ps, 1483232304000L)
    addKill(ps, 1483232305000L)
    addKill(ps, 1483232320000L)
    addKill(ps, 1483232321000L)
    addKill(ps, 1483232321000L)
    addKill(ps, 1483232403000L)
    logger.info(ps.pprint())
  }

}

object PlayerStatesSpec {

  def addKill(ps: PlayerStates, time: Long): Unit = {
    ps.addElement(Array(1,0), time, true)
  }
  def addDeath(ps: PlayerStates, time: Long): Unit = {
    ps.addElement(Array(0,1), time, true)
  }
  def addRpt(ps: PlayerStates, time: Long): Unit = {
    ps.addElement(Array(1,0), time, false)
  }
  def addRpted(ps: PlayerStates, time: Long): Unit = {
    ps.addElement(Array(0,1), time, false)
  }

}