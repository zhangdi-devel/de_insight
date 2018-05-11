/*
 * MIT License
 *
 * Copyright (c) 2018 Zhang Di
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy
 * of this software and associated documentation files (the "Software"), to deal
 * in the Software without restriction, including without limitation the rights
 * to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
 * copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in all
 * copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 * OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
 * SOFTWARE.
 */

package org.dizhang.pubg

import java.sql.{Connection, DriverManager, PreparedStatement}

import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction
import org.dizhang.pubg.UserConfig.Postgres
import PostgresSink._
import org.apache.flink.streaming.runtime.tasks.ProcessingTimeCallback
import org.dizhang.pubg.Stat.SimpleResult

class PostgresSink(postConf: Postgres)
  extends RichSinkFunction[String] with ProcessingTimeCallback {

  private var statement: PreparedStatement = _

  private var batchCount: Int = 0

  private var lastBatchTime = System.currentTimeMillis()

  override def onProcessingTime(timestamp: Long): Unit = {
    statement.executeBatch()
    batchCount = 0
    lastBatchTime = System.currentTimeMillis()
  }

  override def invoke(line: String): Unit = {
    val sr = SimpleResult(line)
    sr.foreach{value =>
      statement.setString(0, value.player)
      statement.setString(1, value.period)
      statement.setLong(2, value.time)
      statement.setInt(3, value.kills)
      statement.setInt(4, value.deaths)
      statement.setInt(5, value.reports)
      statement.setInt(6, value.reported)
      statement.setString(7, value.tag)
      statement.addBatch()
      batchCount += 1

      onProcessingTime(System.currentTimeMillis() + 1000)

      if (shouldExecuteBatch()) {
        statement.executeBatch()
        batchCount = 0
        lastBatchTime = System.currentTimeMillis()
      }

    }
  }

  def shouldExecuteBatch(): Boolean = {
    batchCount >= MAX_BATCH
  }



  override def open(parameters: Configuration): Unit = {
    Class.forName("org.postgresql.Driver")
    val url = s"jdbc:postgresql://${postConf.host}:${postConf.port}/${postConf.db}"
    val connection: Connection =
      DriverManager.getConnection(url, postConf.user, postConf.passwd)
    statement = connection.prepareStatement(UPSERT_RESULT)
  }

}

object PostgresSink {

  private val excluded = "EXCLUDED"

  private val MAX_BATCH = 1000
  private val MAX_INTERVAL = 1000

  private val UPSERT_RESULT = "INSERT INTO stats (player, period, time, kills, deaths, reports, reported, tag) " +
    "VALUES (?, ?, ?, ?, ?, ?, ?, ?) " +
    "ON CONFLICT (playerPeriod) " +
    s"UPDATE SET time = $excluded.time, kills = $excluded.kills, deaths = $excluded.deaths, " +
    s"reports = $excluded.reports, reported = $excluded.reported, tag = $excluded.tag " +
    s"WHERE stats.time < $excluded.time"

}