package com.pingan.pbear.udtf

import java.util.ArrayList

import org.apache.hadoop.hive.ql.exec.UDFArgumentException
import org.apache.hadoop.hive.ql.udf.generic.GenericUDTF
import org.apache.hadoop.hive.serde2.objectinspector.{ObjectInspector, ObjectInspectorFactory, StructObjectInspector}
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory

/**
  * Created by lovelife on 17/1/6.
  * udtf函数通常用来将表中一个字段分解成多个字段：
  * 比如：stream表中line字段分解成name,age字段
  * select parser(line) as (name, age) from stream
  *
  * udtf函数需要继承GenericUDTF，并重写三个虚函数
  */


/**
  * LineParser会将字段由｜分割成多个字段
  */
class LineParser extends GenericUDTF {
  override def close(): Unit = {}

  override def initialize(argOIs: Array[ObjectInspector]): StructObjectInspector = {

    val fieldNames = new ArrayList[String]()
    val fieldOIs = new ArrayList[ObjectInspector]()
    fieldNames.add("col1")
    fieldOIs.add(PrimitiveObjectInspectorFactory.javaStringObjectInspector)
    fieldNames.add("col2")
    fieldOIs.add(PrimitiveObjectInspectorFactory.javaStringObjectInspector)

    return ObjectInspectorFactory.getStandardStructObjectInspector(fieldNames,fieldOIs)
  }

  override def process(objects: Array[AnyRef]): Unit = {

    val input = objects(0).toString()
    val res = input.split("\\|")
    forward(res)
  }
}
