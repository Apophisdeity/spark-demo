package com.person.sbt.demo

import com.person.sbt.demo.sink.FileSink
import com.person.sbt.demo.source.KafkaSource
import com.person.sbt.demo.utils.JobUtils
import org.apache.spark.sql.DataFrame

/**
 * @ClassName WriteDfToJsonFile
 * @author apophis
 * @date 2022/8/10
 * @desc 工程介绍
 */
object WriteDfToJsonFile {

  def main(args: Array[String]): Unit = {
    val env: DataFrame = JobUtils.getEnv(this.getClass.getSimpleName)
    val sourceDf: DataFrame = KafkaSource.source(env)
    FileSink.json(sourceDf, "E:/tmp/KafkaDemo/output", "E:/tmp/KafkaDemo/ck")
  }
}
