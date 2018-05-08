package org.dizhang.pubg


import org.slf4j.{Logger, LoggerFactory}
import java.util.Properties
import java.util.concurrent.TimeUnit

import org.apache.flink.api.common.functions.FlatMapFunction
import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.connectors.kafka.{FlinkKafkaConsumer011, FlinkKafkaProducer011}
import org.apache.flink.util.Collector
import org.dizhang.pubg.Stat.KeyedCounter
import org.dizhang.pubg.StatDescriber.Cnt2

object Analysis {

  val logger: Logger = LoggerFactory.getLogger(getClass)

  def main(args: Array[String]): Unit = {

    logger.info("hey flink")
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
        val simpleGrade = new Cnt2[(Record, Long)](
          ("kills", "deaths"), p => (p._1.event.killer.id, p._1.event.victim.id), p => p._2
        )
        val matchesStream = env.addSource(matches).flatMap{
          new FlatMapFunction[(Record, Long), KeyedCounter] {
            override def flatMap(value: (Record, Long), out: Collector[(String, Long, Array[Int])]): Unit = {
              simpleGrade.fromEvent(value).foreach(r => out.collect(r))
            }
          }
        }.keyBy(0)

        /**
          *
          .assignTimestampsAndWatermarks(
          new BoundedOutOfOrdernessTimestampExtractor[KeyedCounter](Time.seconds(conf.watermark)) {
            override def extractTimestamp(element: KeyedCounter): Long = element._2
          }
        )
          * */

        /*report stream*/
        val simpleCredit = new Cnt2[Report](
          ("reports", "reported"), r => (r.reporter, r.cheater), r => r.time
        )
        val reportsStream = env.addSource(reports).flatMap{
          new FlatMapFunction[Report, KeyedCounter] {
            override def flatMap(value: Report, out: Collector[(String, Long, Array[Int])]): Unit = {
              simpleCredit.fromEvent(value).foreach(r => out.collect(r))
            }
          }
        }.keyBy(0)

        /**
          * .assignTimestampsAndWatermarks(
          new BoundedOutOfOrdernessTimestampExtractor[KeyedCounter](Time.seconds(conf.watermark)) {
            override def extractTimestamp(element: KeyedCounter): Long = element._2
          }
        )
          */

        val names = simpleGrade.names ++ simpleCredit.names

        /** stateful joining */
        val myJoinFunc = new JoinFunction(conf.window, simpleGrade.size, simpleCredit.size)
        val result = reportsStream.connect(matchesStream).flatMap(
          myJoinFunc
          //new JoinFunction(conf.window, simpleGrade.size, simpleCredit.size)
        ).map{r =>
          val cnt = names.zip(r._3).map(p => s"${p._1}:${p._2}")
          s"${r._1},${r._2},${cnt.mkString(",")}"
        }

        /* write back to kafka */
        val producer = new FlinkKafkaProducer011[String](
          conf.brokers.mkString(","),
          "Stats",
          new SimpleStringSchema()
        )
        result.addSink(producer)
        result.print()
        val res = env.execute("Analysis")
        logger.info(s"runtime: ${res.getNetRuntime(TimeUnit.MINUTES)}")
    }
  }
}
