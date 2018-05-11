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
import org.apache.flink.api.common.state.{MapState, MapStateDescriptor}
import org.apache.flink.streaming.api.functions.co.RichCoFlatMapFunction
import org.apache.flink.util.Collector
import org.dizhang.pubg.Stat.KeyedCounter

import scala.collection.mutable

class JoinFunction(window: Map[String, List[Int]], len1: Int, len2: Int)
  extends RichCoFlatMapFunction[KeyedCounter, KeyedCounter, KeyedCounter] {

  /** the history stats for each window */
  lazy val statsBuffer: MapState[String, PlayerStates] = getRuntimeContext.getMapState(
    new MapStateDescriptor[String, PlayerStates]("map states", classOf[String], classOf[PlayerStates])
  )

  /** the early arrived reports
    * this is just for replay
    * in real streaming, it is not necessary in general
    * */
  lazy val creditBuffer: MapState[String, mutable.PriorityQueue[KeyedCounter]] = getRuntimeContext.getMapState(
    new MapStateDescriptor[String, mutable.PriorityQueue[KeyedCounter]](
      "credits", classOf[String], classOf[mutable.PriorityQueue[KeyedCounter]]
    )
  )

  override def flatMap1(value: KeyedCounter, out: Collector[KeyedCounter]): Unit = {

    /** add elements but don't emit results */
    val cnt = value._3
    val time = value._2
    val key = value._1
    window.foreach{
      case (windowName, windows :: windowSize :: _) =>
        /** get the player stats for this window */
        val ps = if (statsBuffer.contains(windowName)) {
          statsBuffer.get(windowName)
        } else {
          new PlayerStates(windows, windowSize, len1, len2)
        }
        ps.addElement(cnt, time, first = true)
        /** get the waiting reports for this window */
        val pq = if (creditBuffer.contains(windowName)) {
          creditBuffer.get(windowName)
        } else {
          val ordering = Ordering.by[KeyedCounter, Long](_._2).reverse
          mutable.PriorityQueue[KeyedCounter]()(ordering)
        }
        val fakeWaterMark = ps.fakeWaterMark
        /** collect old credits */
        while (pq != null && pq.nonEmpty && pq.head._2 < fakeWaterMark) {
          val credit = pq.dequeue()
          ps.addElement(credit._3, credit._2, first = false)
          out.collect((s"$key,$windowName", credit._2, ps.emitElement()))
        }
        creditBuffer.put(windowName, pq)
        statsBuffer.put(windowName, ps)
        //out.collect((s"$key,$windowName", time, statsBuffer.get(windowName).emitElement()))
      case _ => Unit
    }
  }

  override def flatMap2(value: KeyedCounter, out: Collector[KeyedCounter]): Unit = {
    /** add elements and emit results */
    val key = value._1
    window.foreach {
      case (windowName, windows :: windowSize :: _) =>
        val ps = if (statsBuffer.contains(windowName)) {
          statsBuffer.get(windowName)
        } else {
          new PlayerStates(windows, windowSize, len1, len2)
        }
        val pq = if (creditBuffer.contains(windowName)) {
          creditBuffer.get(windowName)
        } else {
          val ordering = Ordering.by[KeyedCounter, Long](_._2).reverse
          mutable.PriorityQueue[KeyedCounter]()(ordering)
        }
        pq.enqueue(value)
        val fakeWaterMark = ps.fakeWaterMark
        while (pq != null && pq.nonEmpty && pq.head._2 < fakeWaterMark) {
          val credit = pq.dequeue()
          ps.addElement(credit._3, credit._2, first = false)
          out.collect((s"$key,$windowName", credit._2, ps.emitElement()))
        }
        statsBuffer.put(windowName, ps)
        creditBuffer.put(windowName, pq)
      case _ => Unit
    }
  }
}
