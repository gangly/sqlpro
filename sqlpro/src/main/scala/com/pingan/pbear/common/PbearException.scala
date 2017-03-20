package com.pingan.pbear.common

/**
  * Created by lovelife on 17/2/18.
  */
object PbearException extends RuntimeException


class WrongCmdPbearException(message: String) extends RuntimeException(message)
class NotFindPbearException(message: String) extends RuntimeException(message)