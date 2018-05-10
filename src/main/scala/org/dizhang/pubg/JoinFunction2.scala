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
import org.apache.flink.streaming.api.functions.co.CoProcessFunction
import org.dizhang.pubg.Stat.{Counter, KeyedCounter}
import JoinFunction2._
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.scala.OutputTag
import org.apache.flink.util.Collector

import scala.collection.mutable

class JoinFunction2(window: Map[String, List[Int]],
                    len1: Int, len2: Int,
                    lateEvent: OutputTag[KeyedCounter])
  extends CoProcessFunction[KeyedCounter, KeyedCounter, KeyedCounter] {

  val windowNames = window.keys.toArray

  def len: Int = len1 + len2

  val counter = new Counter(len)

  /** This is to prevent credit arrive too early
    * Only useful for replay
    * */
  private var creditBufferState: ValueState[mutable.PriorityQueue[KeyedCounter]] = _

  /** times
    *
    * earliest
    * latest
    * watermark?
    *
    * */
  private var timeBoundState: MapState[String, Long] = _

  /** indices and size
    *
    * */
  private var indicesState: MapState[String, Int] = _

  /** current count */
  private var currentState: MapState[String, Array[Int]] = _

  /** history
    * the windows x len matrix is stored in a map
    * idx = windowIdx
    * */
  private var historyState: MapState[String, Array[Int]] = _

  /** initialize everything */
  override def open(parameters: Configuration): Unit = {
    creditBufferState = getRuntimeContext.getState(
      new ValueStateDescriptor[mutable.PriorityQueue[(String, Long, Array[Int])]](
        "credit buffer", classOf[mutable.PriorityQueue[KeyedCounter]]
      )
    )

    timeBoundState = getRuntimeContext.getMapState(
      new MapStateDescriptor[String, Long](
        "times", classOf[String], classOf[Long]
      )
    )

    indicesState = getRuntimeContext.getMapState(
      new MapStateDescriptor[String, Int](
        "indices and size", classOf[String], classOf[Int]
      )
    )

    currentState = getRuntimeContext.getMapState(
      new MapStateDescriptor[String, Array[Int]]("current", classOf[String], classOf[Array[Int]])
    )

    historyState = getRuntimeContext.getMapState(
      new MapStateDescriptor[String, Array[Int]]("history", classOf[String], classOf[Array[Int]])
    )
  }

  def init(): Unit = {
    if (creditBufferState.value() == null) {
      val ordering = Ordering.by[KeyedCounter, Long](_._2).reverse
      val creditBuffer = mutable.PriorityQueue[KeyedCounter]()(ordering)
      creditBufferState.update(creditBuffer)
    }
    if (! timeBoundState.contains(LATEST)) {
      timeBoundState.put(LATEST, 0L)
    }
    windowNames.foreach { w =>
      timeBoundState.put(s"$w-$EARLIEST", 0L)
      indicesState.put(s"$w-$LAST_INDEX", 0)
      currentState.put(w, zero(len))
      (0 until window(w).head).foreach(idx => historyState.put(s"$w-$idx", zero(len)))
    }
  }

  /**read-only methods*/
  def windows(w: String): Int = window(w).head
  def windowSize(w: String): Int = window(w).last
  def earliest(w: String): Long = timeBoundState.get(s"$w-$EARLIEST")
  def latest: Long = timeBoundState.get(LATEST)
  def lastIndex(w: String): Int = indicesState.get(LAST_INDEX)
  def occupiedSize(w: String): Int = ((latest - earliest(w))/(windowSize(w) * 1000) + 1).toInt
  def earliestIndex(w: String): Int = (lastIndex(w) + windows(w) + 1 - occupiedSize(w))%windows(w)

  /** collect current result */
  def emit(w: String): Array[Int] = {
    if (currentState.contains(w)) currentState.get(w) else zero(len)
  }


  def addElement(w: String, cnt: Array[Int], time: Long, first: Boolean): Unit = {
    /** data to be inserted */
    val enriched: Array[Int] =
      if (first)
        cnt ++ zero(len2)
      else
        zero(len1) ++ cnt

    val rawIdx = ((time - earliest(w))/(windowSize(w) * 1000)).toInt
    val currentTime = time/(windowSize(w) * 1000) * (windowSize(w) * 1000)
    if (rawIdx < occupiedSize(w) + windows(w) - 1) {
      /** if fall in range or override part of old data */
      for (i <- 0 to (rawIdx - occupiedSize(w))) {
        val idx = (lastIndex(w) + 1 + i)%windows(w)
        currentState.put(w,
          counter.sub(currentState.get(w), historyState.get(s"$w-$idx"))
        )
        historyState.put(s"$w-$idx", zero(len))
      }
      currentState.put(w,
        counter.add(
          currentState.get(w), enriched
        )
      )
      val idx = (earliestIndex(w) + rawIdx)%windows(w)
      historyState.put(s"$w-$idx",
        counter.add(historyState.get(s"$w-$idx"), enriched)
      )
      timeBoundState.put(LATEST, currentTime)
      timeBoundState.put(s"$w-$EARLIEST",
        if (currentTime - (windowSize(w) * (windows(w) - 1) * 1000) <= earliest(w))
          earliest(w)
        else
          currentTime - (windowSize(w) * (windows(w) - 1) * 1000)
      )
      indicesState.put(s"$w-$LAST_INDEX", idx)
    } else {
      /** throw away old data */
      (0 until windows(w)).foreach(idx => historyState.put(s"$w-$idx", zero(len)))
      historyState.put(s"$w-0", enriched.clone())
      currentState.put(w, enriched.clone())
      timeBoundState.put(s"$w-$EARLIEST", currentTime)
      timeBoundState.put(LATEST, currentTime)
      indicesState.put(s"$w-$LAST_INDEX", 0)
    }
  }

  /** process grade */
  override def processElement1(value: (String, Long, Array[Int]),
                               ctx: CoProcessFunction[KeyedCounter, KeyedCounter, KeyedCounter]#Context,
                               out: Collector[(String, Long, Array[Int])]): Unit = {

    if (creditBufferState.value() == null) init()
    /** check the priority queue of credit */
    val creditBuffer = creditBufferState.value()
    while (creditBuffer != null && creditBuffer.nonEmpty && creditBuffer.head._2 < latest) {
      val credit = creditBuffer.dequeue()
      windowNames.foreach{w =>
        if (credit._2 < earliest(w)) {
          /** waited too long */
          ctx.output(lateEvent, (s"${credit._1},$w", credit._2, zero(len1) ++ credit._3))
        } else if (credit._2 >= earliest(w) && credit._2 < latest) {
          /** put it to history and emit */
          addElement(w, credit._3, credit._2, first = false)
          out.collect((s"${credit._1},$w", credit._2, emit(w)))
        }
      }
    }

    val cnt = value._3
    val time = value._2
    windowNames.foreach{w =>
      if (time < earliest(w)) {
        /** output late event to side output */
        ctx.output(lateEvent, (s"${value._1},$w", time, cnt ++ zero(len2)))
      } else {
        addElement(w, cnt, time, first = true)
      }
    }
  }

  /** process credit */
  override def processElement2(value: (String, Long, Array[Int]),
                               ctx: CoProcessFunction[KeyedCounter, KeyedCounter, KeyedCounter]#Context,
                               out: Collector[(String, Long, Array[Int])]): Unit = {
    if (creditBufferState.value() == null) init()
    val cnt = value._3
    val time = value._2
    if (time >= latest) {
      val creditBuffer = creditBufferState.value()
      creditBuffer.enqueue(value)
    }

    windowNames.foreach{w =>
      if (time < earliest(w)) {
        ctx.output(lateEvent, (value._1, time, zero(len1) ++ cnt))
      } else if (time < latest) {
        addElement(w, cnt, time, first = false)
        out.collect((s"${value._1},$w", time, emit(w)))
      }
    }
  }
}

object JoinFunction2 {

  private def zero(size: Int): Array[Int] = Array.fill(size)(0)
  //private def zero(rows: Int, cols: Int): Array[Array[Int]] = Array.fill(rows)(Array.fill(cols)(0))

  val EARLIEST = "earliest"
  val LATEST = "latest"
  val LAST_INDEX = "lastIndex"

}