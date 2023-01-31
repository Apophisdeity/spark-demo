package com.person.sbt.demo.sink

import org.apache.spark.sql.DataFrame

/**
 * @ClassName ConsoleSink
 * @author apophis
 * @date 2022/8/23
 * @desc 工程介绍
 */
object ConsoleSink {
  def sink(sinkDf: DataFrame): Unit = {
    sinkDf.writeStream
      .outputMode("append")
      .format("console")
      .start()
      .awaitTermination()
  }
}
