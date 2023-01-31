package com.person.sbt.demo

import com.person.sbt.demo.source.KafkaSource
import com.person.sbt.demo.utils.{JobUtils, PropertiesUtils}
import org.apache.spark.sql.DataFrame

import java.util.Properties

/**
 * @ClassName WriteDfToKafka
 * @author apophis
 * @date 2022/8/10
 * @desc 工程介绍
 */
object WriteDfToKafka {

  def main(args: Array[String]): Unit = {
    // https://github.com/cdarlint/winutils 本地运行需要这个
    val env: DataFrame = JobUtils.getEnv(this.getClass.getSimpleName)

    val kafkaDf: DataFrame = KafkaSource.source(env)

    val sourceDf: DataFrame = kafkaDf.selectExpr(exprs =
      "key",
      "to_json(named_struct('offset',offset,'kafkaTime',kafkaTime)) AS value"
    )

    val prop: Properties = PropertiesUtils.get
    sourceDf.writeStream
      .format("kafka")
      .option("kafka.bootstrap.servers", prop.getProperty("kafka.bootstrap.servers"))
      .option("topic", "test")
      .option("checkpointLocation", "E:/tmp/TransformToJson/ck")
      .start()
      .awaitTermination()
    //在kafka中的数据显示为  {"offset":111111,"kafkaTime":1661247760000}


  }
}
