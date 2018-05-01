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

import awscala._
import s3._
import org.apache.commons.compress.compressors.bzip2.BZip2CompressorInputStream
import scala.io.Source

class S3Stream(bucketName: String)
              (implicit s3: S3 = S3Stream.s3) {

  lazy val bucket: Option[Bucket] = s3.bucket(bucketName)

  def get(key: String, codec: String = "plain"): Iterator[String] = {

    val is =
      for {
        b <- bucket
        obj <- b.getObject(key)
      } yield obj.getObjectContent

    is match {
      case None => Iterator.empty
      case Some(s) => codec match {
        case "bzip2" =>
          val bzip2 = new BZip2CompressorInputStream(s)
          Source.fromInputStream(bzip2).getLines()
        case _ =>
          Source.fromInputStream(s).getLines()
      }
    }

  }
}

object S3Stream {

  val region: Region = Region.US_WEST_2
  val s3: S3 = S3.at(region)

}