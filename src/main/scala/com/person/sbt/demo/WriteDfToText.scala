package com.person.sbt.demo

import com.person.sbt.demo.source.KafkaSource
import com.person.sbt.demo.utils.JobUtils
import org.apache.spark.sql.DataFrame

/**
 * @ClassName WriteDfToText
 * @author apophis
 * @date 2022/8/10
 * @desc 工程介绍
 */
object WriteDfToText {

  def main(args: Array[String]): Unit = {
    // https://github.com/cdarlint/winutils 本地运行需要这个
    val env: DataFrame = JobUtils.getEnv(this.getClass.getSimpleName)

    val kafkaDf: DataFrame = KafkaSource.source(env)


    //以结构化形式写入文本,需要合并多列为一列
    kafkaDf.selectExpr(exprs =
      """
        |concat_ws('-',
        |offset,
        |kafkaTime
        |)
        |""".stripMargin
    )
      .writeStream
      .format("text")
      .option("checkpointLocation", "E:/tmp/WriteDfToText/ck")
      .option("path", "E:/tmp/WriteDfToText/output")
      .outputMode("append")
  }.start()
    .awaitTermination()
}
