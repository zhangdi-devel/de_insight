package org.dizhang.pubg


import org.slf4j.{Logger, LoggerFactory}
import java.util.{Properties, UUID}
import java.util.concurrent.TimeUnit
import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.connectors.kafka.{FlinkKafkaConsumer011, FlinkKafkaProducer011}
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
        props.setProperty("group.id", s"${conf.group}.${UUID.randomUUID()}")
        props.setProperty("auto.offset.reset", conf.topic.offset)

        val env = StreamExecutionEnvironment.getExecutionEnvironment
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
        val matches = new FlinkKafkaConsumer011[(Record, Long)](conf.topic.matches, new RecordDeserializer(), props)
        val reports = new FlinkKafkaConsumer011[Report](conf.topic.reports, new ReportDeserializer(), props)

        /*match stream*/
        val simpleGrade = new Cnt2[(Record, Long)](
          ("kills", "deaths"), p => (p._1.event.killer.id, p._1.event.victim.id), p => p._2
        )
        val matchesStream = env.addSource(matches).flatMap{
          r => simpleGrade.fromEvent(r)
        }.keyBy(_._1)

        /*report stream*/
        val simpleCredit = new Cnt2[Report](
          ("reports", "reported"), r => (r.reporter, r.cheater), r => r.time
        )
        val reportsStream = env.addSource(reports).flatMap{
          r => simpleCredit.fromEvent(r)
        }.keyBy(_._1)

        val names = simpleGrade.names ++ simpleCredit.names

        /** stateful joining */
        //val lateEvent = OutputTag[KeyedCounter]("lateEvents")
        val myJoinFunc = new JoinFunction(conf.window, simpleGrade.size, simpleCredit.size)
        val streams = matchesStream.connect(reportsStream).flatMap(
          myJoinFunc
        ).map{r =>
            val cnt = names.zip(r._3).map(p => s"${p._1}:${p._2}")
            val tag =
              if (r._3(3) >= 3)
                "C"
              else if (r._3(2) >= 3)
                "R"
              else
                "N"
            s"${r._1},${r._2},${cnt.mkString(",")},$tag"
        }


        /* write back to kafka */
        val producer = new FlinkKafkaProducer011[String](
          conf.brokers.mkString(","),
          "Stats",
          new SimpleStringSchema()
        )
        streams.addSink(producer)

        /* write results to Postgres */
        val postgresSink = new PostgresSink(conf.postgres)

        streams.addSink(postgresSink)

        val res = env.execute()

        logger.info(s"${res.getNetRuntime(TimeUnit.SECONDS)} seconds")

    }
  }
}
