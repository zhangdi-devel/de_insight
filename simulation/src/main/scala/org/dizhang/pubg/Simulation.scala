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
import org.apache.kafka.clients.admin.{AdminClient, NewTopic}

import collection.JavaConverters._
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

        val adminClient = AdminClient.create(props)

        val partitions = userConf.topic.partitions
        val replicas = userConf.topic.replicas
        val matches = new NewTopic(userConf.topic.matches, partitions, replicas)
        val reports = new NewTopic(userConf.topic.reports, partitions, replicas)

        adminClient.deleteTopics(List(userConf.topic.matches, userConf.topic.reports).asJavaCollection)
        adminClient.createTopics(List(matches, reports).asJavaCollection)

        val producer = new KafkaProducer[String, String](props)

        /* read inputs */
        val s3s: S3Stream = new S3Stream(userConf.sss.bucket)

        userConf.sss.objects.foreach(key => publish(s3s.get(key, userConf.sss.compress), producer)(userConf))
    }
  }

  def publish(input: Iterator[String],
              producer: KafkaProducer[String, String])
             (userConf: UserConfig): Unit = {

    val lambda = 1.0/userConf.delay
    val ed = ExponentialDistribution(lambda)

    val topic = userConf.topic

    /* This PQ holds the reporting events that is simulated to emit in future */
    val unHappened: mutable.PriorityQueue[Report] = new mutable.PriorityQueue[Report]()(Report.ByTime)

    /* match id and idx, to compute the partition */
    var matchId: String = ""
    var matchCnt: Int = 0
    var partition: Int = 0
    input.foreach{ line =>
      val record = Record(line)
      val event = record.event
      val game = record.game
      val killer = event.killer
      val victim = event.victim
      val eventTime = game.date + (event.inGameTime * 1000).toInt

      /* update match id and idx */
      if (event.matchId != matchId) {
        matchId = event.matchId
        matchCnt += 1
        partition = matchCnt/topic.sequential%topic.partitions
      }

      /* emit report for a probability */
      if (Random.nextDouble() < userConf.prob) {
        /* set the reporting time in the future */
        val reportingTime = eventTime + (ed.next() * 1000).toInt
        val report = Report(victim.id, killer.id, game.id, reportingTime)
        unHappened.enqueue(report)
      }

      /* prepare the match event keyed by matchId */
      val matchData = new ProducerRecord[String, String](
        topic.matches, partition, eventTime, matchId, record.toString
      )
      producer.send(matchData)

      /* check if there is any reports to be sent and passed the current time (the match event time)
      * send the reports if they satisfy the conditions
      * */
      while (unHappened.nonEmpty && unHappened.head.time <= eventTime) {
        val report = unHappened.dequeue()
        val reportData = new ProducerRecord[String, String](
          topic.reports, partition, report.time, report.matchId, s"$record,$eventTime"
        )
        producer.send(reportData)
      }

    }
  }

}
