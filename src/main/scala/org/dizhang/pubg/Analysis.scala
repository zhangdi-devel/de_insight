package org.dizhang.pubg


import org.slf4j.{Logger, LoggerFactory}
import java.util.Properties

import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer011
import org.dizhang.pubg.Stat.{Credit, Grade, KeyedCredit, KeyedGrade}

object Analysis {

  val logger: Logger = LoggerFactory.getLogger(getClass)

  def main(args: Array[String]): Unit = {
    UserConfig(args) match {
      case Left(e) =>
        logger.error(s"$e failed to parse config, exit.")
        System.exit(1)
      case Right(conf) =>
        val props = new Properties()
        props.setProperty("bootstrap.servers", conf.brokers.mkString(","))
        props.setProperty("group.id", conf.group)
        props.setProperty("auto.offset.reset", conf.topic.offset)

        val env = StreamExecutionEnvironment.getExecutionEnvironment
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime )
        val matches = new FlinkKafkaConsumer011[(Record, Long)](conf.topic.matches, new RecordDeserializer(), props)
        val reports = new FlinkKafkaConsumer011[Report](conf.topic.reports, new ReportDeserializer(), props)

        /*match stream*/
        val matchesStream = env.addSource(matches).flatMap{cur =>
          val event = cur._1.event
          //val game = cur._1.game

          val time = cur._2
            List(
              KeyedGrade(event.victim.id, Grade(0,1), time),
              KeyedGrade(event.killer.id, Grade(1,0), time)
            )
        }.assignTimestampsAndWatermarks(
          new BoundedOutOfOrdernessTimestampExtractor[KeyedGrade](Time.seconds(conf.watermark)) {
            override def extractTimestamp(element: KeyedGrade): Long = element.time
          }
        ).keyBy(_.player)

        /*report stream*/
        val reportsStream = env.addSource(reports).flatMap{cur =>
          List(
            KeyedCredit(cur.reporter, Credit(1, 0), cur.time),
            KeyedCredit(cur.cheater, Credit(0, 1), cur.time)
          )
        }.assignTimestampsAndWatermarks(
          new BoundedOutOfOrdernessTimestampExtractor[KeyedCredit](Time.seconds(conf.watermark)) {
            override def extractTimestamp(element: KeyedCredit): Long = element.time
          }).keyBy(_.player)

        val result = reportsStream.connect(matchesStream).process(new JoinFunction())


    }
  }
}
