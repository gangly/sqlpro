package com.pingan.pbear.util

import org.scalatest.FlatSpec

/**
  * Created by lovelife on 17/3/5.
  */
class AESUtilSpec extends FlatSpec{

  "AES encode an decode" should " be right" in {
      val content = "p2ppoprssafG"
    assertResult(content) {
      AESUtil.decrypt(AESUtil.encrypt(content))
    }
  }
}
