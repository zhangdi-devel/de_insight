package org.dizhang.pubg.utils
import java.nio.file.{Path, Paths, Files}

import software.amazon.awssdk.core.regions.Region
import software.amazon.awssdk.services.s3.S3Client
import software.amazon.awssdk.services.s3.model.{GetObjectRequest, S3Object}


class S3(bucket: String) {

  def getTo(key: String, offset: Int, length: Int, file: String)
           (implicit s3: S3Client): Unit = {
    val range: String = s"$offset-${offset+length-1}"
    val gorq: GetObjectRequest = GetObjectRequest.builder().bucket(bucket).key(key).range(range).build()
    val path: Path = Paths.get(file)

    s3.getObject(gorq, path)

  }

}

object S3 {
  implicit val region: Region = Region.US_EAST_1
  implicit val s3: S3Client = S3Client.builder().region(region).build()

}