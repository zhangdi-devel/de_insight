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

import java.nio.file.Paths

import com.typesafe.config.ConfigFactory
import org.dizhang.pubg.UserConfig.Topic
import org.slf4j.{Logger, LoggerFactory}

import scala.util.{Failure, Success, Try}

case class UserConfig(group: String,
                      watermark: Int,
                      topic: Topic,
                      window: Map[String, List[Int]],
                      brokers: List[String])

object UserConfig {

  val logger: Logger = LoggerFactory.getLogger(getClass)

  def apply(args: Array[String]) = {
    val default = ConfigFactory.load()
    Try{
      val path = Paths.get(args.head)
      ConfigFactory.parseFile(path.toFile)
    } match {
      case Success(conf) =>
        pureconfig.loadConfig[UserConfig](conf.withFallback(default))
      case Failure(e) =>
        logger.warn(s"${e.toString}\n\tuse default config")
        pureconfig.loadConfig[UserConfig](default)
    }
  }

  case class Topic(matches: String, reports: String, offset: String)
}
