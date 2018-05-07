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

import org.dizhang.pubg.Stat.Counter
import PlayerStates._

/**
  * element1 will be windowed
  * windowSize unit is second
  * */

class PlayerStates(windows: Int,
                   windowSize: Int,
                   len1: Int,
                   len2: Int)(implicit counter: Counter)
  extends Serializable {

  var earliest: Long = 0L
  var latest: Long = 0L
  var lastIndex: Int = 0

  var current: Array[Int] = zero(len)
  var history: Array[Array[Int]] = zero(windows, len)

  def len = len1 + len2
  def earliestIndex: Int = (lastIndex + windows + 1 - occupiedSize)%windows
  def occupiedSize: Int = ((latest - earliest)/(windowSize * 1000) + 1).toInt

  def emitElement(): Array[Int] = current

  /**
    * add element to history
    * update the state
    * */
  def addElement(cnt: Array[Int],
                 time: Long,
                 first: Boolean): Unit = {
    val enriched: Array[Int] =
      if (first)
        counter.merge(cnt, zero(len2))
      else
        counter.merge(zero(len1), cnt)

    val rawIdx = ((time - earliest)/(windowSize * 1000)).toInt
    val currentTime = time/(windowSize * 1000) * (windowSize * 1000)
    if (rawIdx < occupiedSize + windows - 1) {
      /** if fall in range or override part of old data */
      for (i <- 0 to (rawIdx - occupiedSize)) {
        val idx = (lastIndex + 1 + i)%windows
        current = counter.sub(current, history(idx))
        history(idx) = zero(len)
      }
      current = counter.add(current, enriched)
      val idx = (earliestIndex + rawIdx)%windows
      history(idx) = counter.add(history(idx), enriched)
      latest = currentTime
      earliest =
        if (currentTime - (windowSize * (windows - 1) * 1000) <= earliest)
          earliest
        else
          currentTime - (windowSize * (windows - 1) * 1000)
      lastIndex = idx
    } else {
      /** throw away old data */
      history = zero(windows, len)
      history(0) = enriched.clone()
      current = enriched
      earliest = currentTime
      latest = earliest
      lastIndex = 0
    }

  }

}

object PlayerStates {
  private def zero(size: Int): Array[Int] = Array.fill(size)(0)
  private def zero(rows: Int, cols: Int): Array[Array[Int]] = Array.fill(rows)(Array.fill(cols)(0))
}