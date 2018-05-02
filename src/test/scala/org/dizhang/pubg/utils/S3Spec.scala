package org.dizhang.pubg.utils

import org.dizhang.pubg.BaseSpec

class S3Spec extends BaseSpec {

  val bucket = "commoncrawl"
  val obj = "crawl-data/CC-MAIN-2018-13/segments/1521257647612.53/warc/CC-MAIN-20180321102234-20180321122234-00200.warc.gz"

  val s3 = new S3(bucket)

  ignore should "be able to download partial data" in {
    s3.getTo(obj, 711758406, 22460, "user_kakao")(S3.s3)
  }

}
