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
import org.apache.flink.api.common.state.{ValueState, ValueStateDescriptor}
import org.apache.flink.streaming.api.functions.co.RichCoFlatMapFunction
import org.apache.flink.util.Collector
import org.dizhang.pubg.Stat.KeyedCounter

class JoinFunction(window: Map[String, List[Int]], len1: Int, len2: Int)
  extends RichCoFlatMapFunction[KeyedCounter, KeyedCounter, KeyedCounter] {

  lazy val statsBuffer: ValueState[Map[String, PlayerStates]] = getRuntimeContext.getState(
    new ValueStateDescriptor[Map[String, PlayerStates]]("saved stats", classOf[Map[String, PlayerStates]])
  )

  val counter1 = new Stat.Counter(len1)
  val counter2 = new Stat.Counter(len2)

  override def flatMap1(value: KeyedCounter, out: Collector[KeyedCounter]): Unit = {
    val buffer = statsBuffer.value()
    if (statsBuffer == null) {
      val init = window.map{
        case (windowName, windows :: windowSize :: _) =>
          val ps = new PlayerStates(windows, windowSize, len1, len2)(counter1)
          ps.addElement(value._3, value._2, first = true)
          out.collect((s"${value._1},$windowName", value._2, ps.emitElement()))
          (windowName, ps)
      }
      statsBuffer.update(init)
    } else {
      buffer.foreach{
        case (windowName, ps) =>
          ps.addElement(value._3, value._2, first = true)
          out.collect((s"${value._1},$windowName", value._2, ps.emitElement()))
      }
      statsBuffer.update(buffer)
    }
  }

  override def flatMap2(value: KeyedCounter, out: Collector[KeyedCounter]): Unit = {
    val buffer = statsBuffer.value()
    if (statsBuffer == null) {
      val init = window.map{
        case (windowName, windows :: windowSize :: _) =>
          val ps = new PlayerStates(windows, windowSize, len1, len2)(counter2)
          ps.addElement(value._3, value._2, first = false)
          (windowName, ps)
      }
      statsBuffer.update(init)
    } else {
      buffer.foreach{
        case (_, ps) =>
          ps.addElement(value._3, value._2, first = false)
      }
      statsBuffer.update(buffer)
    }
  }

}
