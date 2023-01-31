package com.person.sbt.demo.sink

import org.apache.spark.sql.DataFrame

/**
 * @ClassName ConsoleSink
 * @author apophis
 * @date 2022/8/23
 * @desc 工程介绍
 */
object FileSink {
  def json(sinkDf: DataFrame, path: String, checkpointLocation: String): Unit = {
    sinkDf.writeStream
      .outputMode("append")
      .format("json")
      .option("path", path)
      .option("checkpointLocation", checkpointLocation)
      .start()
      .awaitTermination()
  }

  def text(sinkDf: DataFrame, path: String, checkpointLocation: String): Unit = {
    sinkDf.writeStream
      .outputMode("append")
      .format("text")
      .option("path", path)
      .option("checkpointLocation", checkpointLocation)
      .start()
      .awaitTermination()
  }
}
