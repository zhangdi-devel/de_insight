/*
 *    Copyright 2018 Zhang Di
 *
 *    Licensed under the Apache License, Version 2.0 (the "License");
 *    you may not use this file except in compliance with the License.
 *    You may obtain a copy of the License at
 *
 *        http://www.apache.org/licenses/LICENSE-2.0
 *
 *    Unless required by applicable law or agreed to in writing, software
 *    distributed under the License is distributed on an "AS IS" BASIS,
 *    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *    See the License for the specific language governing permissions and
 *    limitations under the License.
 */

package org.dizhang.pubg

import scala.util.Random
/*
* This is a simple implementation of the exponential distribution
*
* useful to simulate the waiting time
*
* */
class ExponentialDistribution(lambda: Double, seed: Long) {

  val random: Random = new Random(seed)

  def pdf(x: Double): Double = if (x < 0) 0 else {
    lambda * math.exp(-lambda * x)
  }

  def cdf(x: Double): Double = if (x < 0) 0 else {
    1 - math.exp(-lambda * x)
  }

  def quantile(q: Double): Double = {
    require(q >= 0 && q <= 1, "q must belong to [0,0, 1.0]")
    - math.log(1 - q)/lambda
  }

  def next(): Double = {
    quantile(random.nextDouble())
  }
}

object ExponentialDistribution {

  def apply(lambda: Double): ExponentialDistribution = {
    new ExponentialDistribution(lambda, System.currentTimeMillis())
  }

  def apply(lambda: Double, seed: Long): ExponentialDistribution =
    new ExponentialDistribution(lambda, seed)


}