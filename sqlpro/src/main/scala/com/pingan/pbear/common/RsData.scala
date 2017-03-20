package com.pingan.pbear.common

/**
  * Created by zhangrunqin on 16-11-19.
  */
sealed trait RsData
case class SubTask(mainTag: String, subTag: String, options: Map[String, String], index: Int) extends RsData with Serializable
case class NewsVector(
                     newsId: Int, //新闻ID
                     size: Int, //稀疏向量维度,此字段可以去掉
                     indices: Array[Int], //位置索引
                     values: Array[Double],
                     tagIds: Array[Int], //标签列表
                     categoryIds: Array[Int], //列表列表
                     createDate: Long //直接存储时间戳,避免日期的比较,直接比较数值即可
                   ) extends RsData with Serializable

case class UserVector(
                       clientNo: Int, //用户ID
                       size: Int, //稀疏向量维度
                       indices: Array[Int], //位置索引
                       values: Array[Double]
                     ) extends RsData with Serializable
