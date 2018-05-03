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

class RecordSpec extends BaseSpec {
  val line = "2U4GBNA0Ymm6YY-DI35qiTfWmn7xFVDqLSWNAAchnjO2v1-5YHTTVcfx077UIFao,ERANGEL,Pikard,25.0,295350.3,146474.6,DoWork-_x,49.0,0.0,0.0,Punch,120.0,1485321995000,49,2,tpp"

  "A Record" should "be parsed" in {
    val rec = Record(line)

    logger.info(s"event: ${rec.event}")
    logger.info(s"game: ${rec.game}")
    logger.info(s"killer: ${rec.event.killer}")
    logger.info(s"victim: ${rec.event.victim}")

  }
}
