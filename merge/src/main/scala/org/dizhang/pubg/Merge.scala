package org.dizhang.pubg

import org.apache.spark.sql.{DataFrame, SparkSession}
import java.text.SimpleDateFormat
import java.util.Date
import io.circe._, io.circe.generic.auto._, io.circe.parser._, io.circe.syntax._

object Merge {

  val SCALE = 10000

  def main(args: Array[String]): Unit = {

    implicit val spark = SparkSession.builder().appName("Merge").getOrCreate()

    val death = readCsv("s3a://zhangdi-insight/pubg/death.*.csv")

    val agg = readCsv("s3a://zhangdi-insight/pubg/agg.*.csv")

    val sdf = new SimpleDateFormat("yyyy-mm-dd-hh:mm:ss")
    /* unique matches */
    val matches = agg.rdd.map{row =>
      val date: Date = sdf.parse(row.getString(0).replaceFirst("T", "-"))
      val gameSize = row.getString(1).toInt
      val id = row.getString(2)
      val mode = row.getString(3)
      val partySize = row.getString(4).toInt
      (id, Match(date, gameSize, id, mode, partySize))
    }.reduceByKey((a, _) => a)

    val start: Date = matches.map(_._2.date).min()
    val data =
      death.rdd.map{row =>
        val s = row.toSeq.map(v => v.asInstanceOf[String])
        val killer = Player(s.slice(1, 5))
        val victim = Player(s.slice(8, 12))
        val event = Event(s(0), killer, s(5), s(6), s(7).toDouble, victim)
        (event.matchId, event)
      }.join(matches).map{
        case (_, (e, m)) =>



      }

  }

  def readCsv(path: String)(implicit spark: SparkSession): DataFrame = {
    spark.read.format("csv").option("header", "true").load(path)
  }

}
