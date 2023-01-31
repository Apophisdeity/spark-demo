package com.person.sbt.demo.utils

import org.apache.spark.sql.{DataFrame, SparkSession}

import java.util.Properties

/**
 * @ClassName JobUtils
 * @author apophis
 * @date 2022/8/23
 * @desc 工程介绍
 */
object JobUtils {
  def getEnv(jobName: String): DataFrame = {
    // https://github.com/cdarlint/winutils 本地运行需要这个
    System.setProperty("hadoop.home.dir", "C:/dev/winutils/hadoop-2.8.2")
    val prop: Properties = PropertiesUtils.get
    val spark: SparkSession = SparkSession.builder()
      .master("local[1]")
      .appName(jobName)
      .getOrCreate()

    val sourceDf: DataFrame = spark.readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", prop.getProperty("kafka.bootstrap.servers"))
      .option("subscribe", prop.getProperty("subscribe"))
      .load()
    sourceDf
  }
}
