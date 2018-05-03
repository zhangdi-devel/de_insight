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

import org.slf4j.{Logger, LoggerFactory}
import java.nio.file.{Files, Path, Paths}
import java.util.Properties

import com.typesafe.config.ConfigFactory
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}

import scala.collection.mutable
import scala.util.{Failure, Random, Success, Try}
/*
*
* This program will simulate stream of reporting events
* from match deaths data
*
* */
object Simulation {

  val logger: Logger = LoggerFactory.getLogger(getClass)

  def main(args: Array[String]): Unit = {

    val default = ConfigFactory.load()
    val rawConf =
      (Try(Paths.get(args(0))) match {
        case Failure(e) =>
          logger.warn(s"${e.getMessage}\n\twill use default conf")
          default
        case Success(path) =>
          ConfigFactory.parseFile(path.toFile).withFallback(default).resolve()
      }).getConfig("simulation")


    UserConfig(rawConf) match {
      case Left(e) =>
        logger.error(e.toString)
        System.exit(1)
      case Right(userConf) =>
        /* set kafka properties */
        val brokers = userConf.brokers.mkString(",")

        val props = new Properties()
        props.put("bootstrap.servers", brokers)
        props.put("client.id", "SimulateReporting")
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer")
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer")

        val producer = new KafkaProducer[String, String](props)

        /* read inputs */
        val s3s: S3Stream = new S3Stream(userConf.sss.bucket)

        userConf.sss.objects.foreach(key => publish(s3s.get(key, userConf.sss.compress), producer)(userConf))
    }
  }

  def publish(input: Iterator[String],
              producer: KafkaProducer[String, String])
             (userConf: UserConfig): Unit = {
    val rnd = new Random()

    val topic = userConf.topic
    /* process lines */
    var matchId: String = ""
    val players: mutable.ArrayBuffer[(String, (String, Long))] = new mutable.ArrayBuffer[(String, (String, Long))]()

    input.foreach{line =>
      val record: Record = Record(line)
      if (record.event.matchId != matchId) {
        rnd.shuffle(players.toList).take(players.length/10).foreach{
          case (victim, (killer, time)) =>
            /* at anytime in the next two days */
            val reportTime = time + rnd.nextInt(172800)/userConf.scale
            val data =
              new ProducerRecord[String, String](
                topic.reports, topic.partition, reportTime, matchId, s"$victim,$killer"
              )
            producer.send(data)
        }
        players.clear()
        matchId = record.event.matchId

      }
      val event = record.event
      val killer = event.killer
      val victim = event.victim
      val game = record.game
      val eventTime = userConf.start + (game.date - userConf.start + event.time.toInt)/userConf.scale
      val data = new ProducerRecord[String, String](
        topic.matches, topic.partition, eventTime, event.matchId, record.toString
      )
      players += (victim.id -> (killer.id -> eventTime))
      producer.send(data)
    }
  }

}
