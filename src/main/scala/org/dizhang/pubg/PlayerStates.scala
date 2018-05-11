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


import java.time.ZoneId
import java.util.Date

import org.dizhang.pubg.Stat.Counter
import PlayerStates._
import org.slf4j.{Logger, LoggerFactory}

/**
  * elements will be aggregated to windows
  * windowSize unit is second
  * */

class PlayerStates(windows: Int,
                   windowSize: Int,
                   len1: Int,
                   len2: Int)
  extends Serializable {

  val logger: Logger = LoggerFactory.getLogger(getClass)

  var fakeWaterMark: Long = 0L

  var earliest: Long = 0L
  var latest: Long = 0L
  var lastIndex: Int = 0

  var current: Array[Int] = zero(len)
  var history: Array[Array[Int]] = zero(windows, len)

  val counter: Counter = new Counter(len)

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
    if (time >= earliest) {
      fakeWaterMark = math.max(fakeWaterMark, time)
      logger.debug(s"time: ${time.utc}, earliest: ${earliest.utc}")
      /** data to be inserted */
      val enriched: Array[Int] =
        if (first)
          cnt ++ zero(len2)
        else
          zero(len1) ++ cnt

      val rawIdx = ((time - earliest)/(windowSize * 1000)).toInt
      logger.debug(s"rawIdx: $rawIdx, occupiedSize: $occupiedSize, windows: $windows")
      val currentTime = time/(windowSize * 1000) * (windowSize * 1000)
      logger.debug(s"currentTime (slot): $currentTime ${currentTime.utc}")
      if (rawIdx < occupiedSize + windows - 1) {
        /** if fall in range or override part of old data */
        for (i <- 0 to (rawIdx - occupiedSize)) {
          val idx = (lastIndex + 1 + i)%windows
          current = counter.sub(current, history(idx))
          history(idx) = zero(len)
        }
        current = counter.add(current, enriched)
        val idx = (earliestIndex + rawIdx)%windows
        logger.debug(s"idx: $idx")
        history(idx) = counter.add(history(idx), enriched)
        logger.debug(s"overlap idx?: ${currentTime - (windowSize.toLong * (windows - 1) * 1000)}")
        earliest = math.max(earliest, currentTime - (windowSize.toLong * (windows - 1) * 1000))
        if (currentTime > latest) {
          latest = currentTime
          lastIndex = idx
        }
      } else {
        /** throw away old data */
        history = zero(windows, len)
        history(0) = enriched.clone()
        current = enriched.clone()
        earliest = currentTime
        latest = earliest
        lastIndex = 0
      }
    } else {
      logger.debug(s"time ${time.utc} < ${earliest.utc}")
    }
  }

  def pprint(): String = {
    s"""
       |windows: $windows
       |windowSize: $windowSize
       |earliest: ${new Date(earliest)}
       |latest: ${new Date(latest)}
       |earliestIndex: ${earliestIndex}
       |lastIndex: $lastIndex
       |occupiedSize: $occupiedSize
       |current: ${current.mkString(",")}
       |history:
       |${history.zipWithIndex.map{case (r, i) => s"\t$i: ${r.mkString(",")}"}.mkString("\n")}
       |""".stripMargin
  }

}

object PlayerStates {
  private def zero(size: Int): Array[Int] = Array.fill(size)(0)
  private def zero(rows: Int, cols: Int): Array[Array[Int]] = Array.fill(rows)(Array.fill(cols)(0))

  implicit class utctime(val time: Long) extends AnyVal {
    def utc = new Date(time).toInstant.atZone(ZoneId.of("UTC")).toString
  }

}