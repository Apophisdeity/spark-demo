package com.person.sbt.demo

import com.person.sbt.demo.utils.PropertiesUtils
import org.apache.spark.sql.streaming.Trigger
import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}

import java.util.{Calendar, Properties}

/**
 * @ClassName WriteDfToText
 * @author apophis
 * @date 2022/8/10
 * @desc 工程介绍
 */
object ReadTimer {

  def main(args: Array[String]): Unit = {
    System.setProperty("hadoop.home.dir", "C:/dev/winutils/hadoop-2.8.2")
    val prop: Properties = PropertiesUtils.get
    val spark: SparkSession = SparkSession.builder()
      .master("local[1]")
      .appName(this.getClass.getSimpleName)
      .getOrCreate()

    //首次初始化
    var staticDF: DataFrame = spark.read
      .format("jdbc")
      .option("url", prop.getProperty("cc.qa.mysql.jdbc.url"))
      .option("dbtable", prop.getProperty("schema.table"))
      .option("user", prop.getProperty("cc.qa.mysql.username"))
      .option("password", prop.getProperty("cc.qa.mysql.password"))
      .load()
    staticDF.persist()
    println(s"初始化读取MySQL ${Calendar.getInstance().getTime}")

    import spark.implicits._
    // rate stream 在某种意义上相当于定时器
    val staticRefreshStream: Dataset[Long] = spark.readStream
      .format("rate")
      .option("rowsPerSecond", 1)
      .option("numPartitions", 1)
      .load()
      .selectExpr("CAST(value as LONG) as trigger")
      .as[Long]

    //  2. Define a method that refreshes the static Dataframe
    def foreachBatchMethod[T](staticDf: Dataset[T], batchId: Long) = {
      staticDf.unpersist()

      staticDF = spark.read
        .format("jdbc")
        .option("url", prop.getProperty("cc.qa.mysql.jdbc.url"))
        .option("dbtable", prop.getProperty("schema.table"))
        .option("user", prop.getProperty("cc.qa.mysql.username"))
        .option("password", prop.getProperty("cc.qa.mysql.password"))
        .load()
      staticDf.persist()
      println(s"定时读取MySQL ${Calendar.getInstance().getTime}: Refreshing static Dataframe from DeltaLake")
    }


    // 5. Within that Rate Stream have a `foreachBatch` sink that calls refresher method
    staticRefreshStream.writeStream
      .outputMode("append")
      .foreachBatch(foreachBatchMethod[Long] _)
      .queryName("RefreshStream")
      .trigger(Trigger.ProcessingTime("10 seconds"))
      .start()
      .awaitTermination()
  }
}