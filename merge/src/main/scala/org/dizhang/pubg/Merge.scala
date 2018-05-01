package org.dizhang.pubg

import org.apache.spark.sql.{DataFrame, SparkSession}
import java.text.SimpleDateFormat
import java.util.Date
import org.apache.hadoop.io.compress.Lz4Codec
import org.slf4j.LoggerFactory

object Merge {

  val logger = LoggerFactory.getLogger(getClass)

  def main(args: Array[String]): Unit = {

    implicit val spark = SparkSession.builder().appName("Merge").getOrCreate()

    val death = readCsv("s3a://zhangdi-insight/pubg/death.*.csv")

    val agg = readCsv("s3a://zhangdi-insight/pubg/agg.*.csv")

    val sdf = new SimpleDateFormat("yyyy-mm-dd'T'hh:mm:ss'+'SSSS")
    /* unique matches */
    val matches = agg.rdd.map{row =>
      val date: Long = sdf.parse(row.getString(0)).getTime
      val gameSize = row.getString(1).toInt
      val id = row.getString(2)
      val mode = row.getString(3)
      val partySize = row.getString(4).toInt
      (id, Match(date, gameSize, id, mode, partySize))
    }.reduceByKey((a, _) => a)

    matches.cache()

    val start: Long = matches.map(_._2.date).min()

    logger.info(s"start time: $start ${sdf.format(new Date(start))}")

    val data =
      death.rdd.flatMap{row =>
        val s = row.toSeq.map(v => v.asInstanceOf[String])
        if (s.length < 12 || s.contains(null)) {
          None
        } else {
          val killer = Player(s.slice(1, 5))
          val victim = Player(s.slice(8, 12))
          val event = Event(s(0), killer, s(5), s(6), s(7).toDouble, victim)
          Some(event.matchId -> event)
        }
      }.join(matches).map{
        case (_, (e, m)) => s"$e,$m"
      }
    data.saveAsTextFile("s3a://zhangdi-insight/pubg/merged.json.lz4", classOf[Lz4Codec])

  }

  def readCsv(path: String)(implicit spark: SparkSession): DataFrame = {
    spark.read.format("csv")
      .option("header", "true")
      .load(path)
  }

}
