package com.pingan.pbear.util

import java.lang.reflect.{ParameterizedType, Type}

import com.fasterxml.jackson.core.`type`.TypeReference
import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.scala.DefaultScalaModule

/**
  * Created by zhangrunqin on 16-11-18.
  */
object JsonUtil {

  val mapper = new ObjectMapper()
  mapper.registerModule(DefaultScalaModule)

  /**
    * Json -> 对象 的转换
    *
    * @param value Json字符串
    * @tparam T 类型
    * @return 类型对象
    */
  def fromJson[T: Manifest](value: String): T = {
    mapper.readValue(value, typeReference[T])
  }

  /**
    * Json <- 对象 的转换
    *
    * @param value Any type 对象
    * @param prettyFmt default = false
    * @return Json字符串
    */
  def toJson(value: Any, prettyFmt: Boolean = false): String = {
    if (prettyFmt) {
      mapper.writerWithDefaultPrettyPrinter().writeValueAsString(value)
    } else {
      import java.io.StringWriter
      val writer = new StringWriter()
      mapper.writeValue(writer, value)
      writer.toString
    }
  }

  /**
    * 辅助函数
    */
  private[this] def typeReference[T: Manifest] = new TypeReference[T] {
    override def getType = typeFromManifest(manifest[T])
  }

  /**
    * 辅助函数
    */
  private[this] def typeFromManifest(m: Manifest[_]): Type = {
    if (m.typeArguments.isEmpty) { m.runtimeClass }
    else new ParameterizedType {
      def getRawType = m.runtimeClass
      def getActualTypeArguments = m.typeArguments.map(typeFromManifest).toArray
      def getOwnerType = null
    }
  }

}
