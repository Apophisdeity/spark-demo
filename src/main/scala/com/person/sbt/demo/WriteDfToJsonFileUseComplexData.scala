package com.person.sbt.demo

import com.person.sbt.demo.sink.FileSink
import com.person.sbt.demo.source.KafkaSource
import com.person.sbt.demo.utils.JobUtils
import org.apache.spark.sql.DataFrame

/**
 * @ClassName WriteDfToJsonFileUseComplexData
 * @author apophis
 * @date 2022/8/10
 * @desc 工程介绍
 */
object WriteDfToJsonFileUseComplexData {

  def main(args: Array[String]): Unit = {
    // https://github.com/cdarlint/winutils 本地运行需要这个
    val env: DataFrame = JobUtils.getEnv(this.getClass.getSimpleName)

    val kafkaDf: DataFrame = KafkaSource.source(env)

    // header 属于二级嵌套
    val sourceDf: DataFrame = kafkaDf.selectExpr(exprs =
      "key",
      "kafkaTime",
      "to_json(named_struct('key', key,'offset',offset)) AS header"
    )

    FileSink.text(sourceDf, "E:/tmp/TransformToJson/output", "E:/tmp/TransformToJson/ck")

  }
}
