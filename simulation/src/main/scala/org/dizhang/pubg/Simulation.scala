package org.dizhang.pubg

import org.slf4j.{Logger, LoggerFactory}
import java.nio.file.{Files, Path, Paths}
import java.util.Properties
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}
import scala.collection.mutable
import scala.io.Source
import scala.util.Random
/*
*
* This program will simulate stream of reporting events
* from match deaths data
*
* */
object Simulation {

  val logger: Logger = LoggerFactory.getLogger(getClass)

  def main(args: Array[String]): Unit = {
    /* check input */
    if (args.length < 1) {
      logger.error("Please provide an input file")
      System.exit(1)
    }

    val path: Path = Paths.get(args(0))

    if (! Files.exists(path) || ! Files.isRegularFile(path)) {
      logger.error(s"File doesn't exists or not a regular file: ${args(0)}")
      System.exit(1)
    }

    /* set properties */
    val topic1 = "Match"
    val topic2 = "Report"
    val brokers = "localhost:9092"
    val rnd = new Random()
    val props = new Properties()
    props.put("bootstrap.servers", brokers)
    props.put("client.id", "SimpleReporting")
    props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer")
    props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer")

    val producer = new KafkaProducer[String, String](props)

    /* read input */
    val input = Source.fromFile(args(0)).getLines()

    /* process lines */
    var matchId: String = ""

    val players: mutable.ArrayBuffer[(String, String)] = new mutable.ArrayBuffer[(String, String)]()

    input.foreach{line =>
      val s = line.split(",")
      if (s(6) != matchId) {
        Random.shuffle(players.toList).take(players.length/10).foreach{
          case (v, k) =>
            val data = new ProducerRecord[String, String](topic2, v, k)
            producer.send(data)
        }
        players.clear()
        matchId = s(6)
      }
      val killer = s(1)
      val victim = s(8)
      val data = new ProducerRecord[String, String](topic1, killer, s"$killer,$victim")
      players += (victim -> killer)
      producer.send(data)
    }
  }
}
