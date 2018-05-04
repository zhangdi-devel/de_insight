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
        val env = StreamExecutionEnvironment.getExecutionEnvironment
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime )
        val matches = new FlinkKafkaConsumer011[(Record, Long)](conf.topic.matches, new RecordDeserializer(), props)
        val reports = new FlinkKafkaConsumer011[Report](conf.topic.reports, new ReportDeserializer(), props)

        val matchesStream = env.addSource(matches).flatMap{cur =>
          val event = cur._1.event
          //val game = cur._1.game
          val time = cur._2
            List(
              (event.victim.id, Grade(0,1), time),
              (event.killer.id, Grade(1,0), time)
            )
        }.assignTimestampsAndWatermarks(
          new BoundedOutOfOrdernessTimestampExtractor[(String, Grade, Long)](Time.seconds(10)) {
            override def extractTimestamp(element: (String, Grade, Long)): Long = element._3
          }
        ).keyBy(_._1).timeWindow(Time.hours(1)).reduce((a, b) =>
          (a._1, a._2 ++ b._2, math.min(a._3, b._3))
        ).map(p => KeyedGrade(p._1, p._2, p._3)).keyBy(_.player)

        val reportsStream = env.addSource(reports).flatMap{cur =>
          List(
            (cur.reporter, Credit(1, 0), cur.time),
            (cur.cheater, Credit(0, 1), cur.time)
          )
        }.map(p => KeyedCredit(p._1, p._2, p._3)).assignTimestampsAndWatermarks(
          new BoundedOutOfOrdernessTimestampExtractor[KeyedCredit](Time.seconds(10)) {
            override def extractTimestamp(element: KeyedCredit): Long = element.time
          }).keyBy(_.player)

        val result = reportsStream.connect(matchesStream).flatMap(new JoinFunction())


    }
  }
}
