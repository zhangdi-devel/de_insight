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
import org.apache.flink.api.common.state.{MapState, MapStateDescriptor, ValueState, ValueStateDescriptor}
import org.apache.flink.streaming.api.functions.co.RichCoFlatMapFunction
import org.apache.flink.util.Collector
import org.dizhang.pubg.Stat.KeyedCounter

class JoinFunction(window: Map[String, List[Int]], len1: Int, len2: Int)
  extends RichCoFlatMapFunction[KeyedCounter, KeyedCounter, KeyedCounter] {

  lazy val statsBuffer: ValueState[Map[String, PlayerStates]] = getRuntimeContext.getState(
    new ValueStateDescriptor[Map[String, PlayerStates]]("saved stats", classOf[Map[String, PlayerStates]])
  )

  lazy val stateBuffer: MapState[String, PlayerStates] = getRuntimeContext.getMapState(
    new MapStateDescriptor[String, PlayerStates]("map states", classOf[String], classOf[PlayerStates])
  )

  override def flatMap1(value: KeyedCounter, out: Collector[KeyedCounter]): Unit = {
    /** add elements but don't emit results */
    val buffer = statsBuffer.value()
    if (buffer == null) {
      val init = window.flatMap{
        case (windowName, windows :: windowSize :: _) =>
          val ps = new PlayerStates(windows, windowSize, len1, len2)
          ps.addElement(value._3, value._2, first = true)
          out.collect((s"${value._1},$windowName", value._2, ps.emitElement()))
          Some((windowName, ps))
        case _ => None
      }
      statsBuffer.update(init)
    } else {
      val res =
      buffer.map{
        case (windowName, ps) =>
          ps.addElement(value._3, value._2, first = true)
          (s"${value._1},$windowName", value._2, ps.emitElement())
      }
      res.foreach(r => out.collect(r))
      //statsBuffer.update(buffer)
    }
  }

  override def flatMap2(value: KeyedCounter, out: Collector[KeyedCounter]): Unit = {
    /** add elements and emit results */
    val buffer = statsBuffer.value()
    if (buffer == null) {
      val init = window.flatMap{
        case (windowName, windows :: windowSize :: _) =>
          val ps = new PlayerStates(windows, windowSize, len1, len2)
          ps.addElement(value._3, value._2, first = false)
          out.collect((s"${value._1},$windowName", value._2, ps.emitElement()))
          Some((windowName, ps))
        case _ => None
      }
      statsBuffer.update(init)
    } else {
      val res =
      buffer.map{
        case (windowName, ps) =>
          ps.addElement(value._3, value._2, first = false)
          (s"${value._1},$windowName", value._2, ps.emitElement())
      }
      res.foreach(r => out.collect(r))
      //statsBuffer.update(buffer)
    }
  }


}
