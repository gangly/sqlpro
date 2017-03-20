package com.pingan.pbear.udf.common

import org.scalatest.FlatSpec

/**
  * Created by lovelife on 17/3/5.
  */
class GeoIpSpec extends FlatSpec{
  ignore should " ip transform to right country or city" in {
    assertResult("北京") {
      GeoIp.ip2city("61.135.169.121")
    }

    assertResult("中国") {
      GeoIp.ip2country("61.135.169.121")
    }
  }
}
