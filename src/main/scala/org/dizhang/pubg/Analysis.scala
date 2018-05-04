package org.dizhang.pubg

import org.slf4j.{Logger, LoggerFactory}
import java.util.Properties

import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer011

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
        val matches = new FlinkKafkaConsumer011[String](conf.topic.matches, new SimpleStringSchema(), props)
        val reports = new FlinkKafkaConsumer011[String](conf.topic.reports, new SimpleStringSchema(), props)

        val matchesStream = env.addSource(matches)
        val reportsStream = env.addSource(reports)

        matchesStream.flatMap{line =>
          val record = Record(line)

        }
    }
  }
}
