package org.dizhang.pubg

import org.scalatest._
import org.slf4j.{Logger, LoggerFactory}

abstract class BaseSpec extends FlatSpec with Matchers with
  OptionValues with Inside with Inspectors {
  def logger: Logger = LoggerFactory.getLogger(getClass)
}
