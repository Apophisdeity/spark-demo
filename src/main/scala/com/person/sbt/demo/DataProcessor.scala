package com.person.sbt.demo

import com.person.sbt.demo.source.KafkaSource
import com.person.sbt.demo.utils.PropertiesUtils
import org.apache.spark.sql._
import org.apache.spark.sql.streaming.Trigger

import java.util.{Calendar, Properties}

/**
 * @ClassName WriteDfToText
 * @author apophis
 * @date 2022/8/10
 * @desc 工程介绍
 */
object DataProcessor {

  def main(args: Array[String]): Unit = {
    System.setProperty("hadoop.home.dir", "C:/dev/winutils/hadoop-2.8.2")
    val prop: Properties = PropertiesUtils.get
    val spark: SparkSession = SparkSession.builder()
      .master("local[1]")
      .appName(this.getClass.getSimpleName)
      .getOrCreate()

    val sourceDf: DataFrame = spark.readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", prop.getProperty("kafka.bootstrap.servers"))
      .option("subscribe", prop.getProperty("subscribe"))
      .load()

    val kafkaDf: DataFrame = KafkaSource.source(sourceDf)
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

    val mapPartitions: Dataset[String] = kafkaDf.as[SourceModel](Encoders.product[SourceModel])
      .mapPartitions(partition => {
        partition.map(x => "").iterator
      })

    val map: Dataset[String] = kafkaDf.as[SourceModel](Encoders.product[SourceModel])
      .map(x => {
        ""
      })


    //以结构化形式写入文本,需要合并多列为一列
    val function: (Dataset[Row], Long) => Unit = (batchDF: Dataset[Row], batchId: Long) => {
      batchDF.persist()
      batchDF.write.format("text").mode("append").save("E:/tmp/WriteDfToForeach/output1")
      batchDF.write.format("text").mode("append").save("E:/tmp/WriteDfToForeach/output2")
      batchDF.unpersist()
    }
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
      .foreachBatch(function)
      .outputMode("append")
  }.start()
    .awaitTermination()

  case class SourceModel(key: String, value: String, partition: Int, offset: Long, kafkaTime: Long)

  case class CustomSourceModel(key: String, value: String, partition: Int, offset: Long, kafkaTime: Long)

}