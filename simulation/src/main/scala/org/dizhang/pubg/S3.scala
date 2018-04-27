package org.dizhang.pubg

import java.nio.file.{Path, Paths, Files}

import software.amazon.awssdk.core.regions.Region
import software.amazon.awssdk.services.s3.S3Client
import software.amazon.awssdk.services.s3.model.{GetObjectRequest, S3Object}


class S3(bucket: String) {

  def get(key: String)
         (implicit s3: S3Client) = {

    val gorq: GetObjectRequest = GetObjectRequest.builder().bucket(bucket).key(key).build()

    s3.getObject(gorq)

  }

}

object S3 {
  implicit val region: Region = Region.US_WEST_2
  implicit val s3: S3Client = S3Client.builder().region(region).build()

}