package com.person.sbt.demo.source

import org.apache.spark.sql.DataFrame

/**
 * @ClassName KafkaSource
 * @author apophis
 * @date 2022/8/23
 * @desc 工程介绍
 */
object KafkaSource {
  def source(sourceDf: DataFrame): DataFrame = {
    val kafkaDf: DataFrame = sourceDf.selectExpr(exprs =
      "cast(key AS STRING)",
      "cast(value AS STRING)",
      "partition",
      "offset",
      "unix_timestamp(timestamp,'yyyy-MM-dd HH:mm:ss.SSS')*1000 AS kafkaTime"
    )
    kafkaDf
  }
}
