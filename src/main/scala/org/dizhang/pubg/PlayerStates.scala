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

import org.dizhang.pubg.Stat.{Credit, Grade}

class PlayerStates(windows: Int, windowSize: Int, start: Long) {

  var earliest: Long = start

  var kills: Int = 0
  var deaths: Int = 0
  var report: Int = 0
  var reported: Int = 0

  private def zero(): Array[Array[Int]] = Array.fill(4)(Array.fill(windows)(0))

  var history: Array[Array[Int]] = zero()

  def addGrade(grade: Grade, time: Long): Unit = {
    val idx: Int = ((time - earliest)/windowSize).toInt
    if (idx >= 0 && idx < windows) {
      history(0)(idx) += grade.kills
      history(1)(idx) += grade.deaths
      kills += grade.kills
      deaths += grade.deaths
    }
  }

  def addCredit(credit: Credit, time: Long): Unit = {
    val idx: Int = ((time - earliest)/windowSize).toInt
    if (idx >= 0 && idx < windows) {
      history(2)(idx) += credit.report
      history(3)(idx) += credit.reported
      report += credit.report
      reported += credit.reported
    }
  }

  def rollHistory(steps: Int): Unit = {
    require(steps > 0)
    if (steps >= windows) {
      history = zero()
      kills = 0
      deaths = 0
      report = 0
      reported = 0
    } else {
      val newHistory = zero()
      (0 until windows).foreach{ idx =>
        if (idx < steps) {
          kills -= history(0)(idx)
          deaths -= history(1)(idx)
          report -= history(2)(idx)
          reported -= history(3)(idx)
        } else {
          newHistory(0)(idx - steps) = history(0)(idx)
          newHistory(1)(idx - steps) = history(1)(idx)
          newHistory(2)(idx - steps) = history(2)(idx)
          newHistory(3)(idx - steps) = history(3)(idx)
        }
      }
      history = newHistory
    }
  }

}
