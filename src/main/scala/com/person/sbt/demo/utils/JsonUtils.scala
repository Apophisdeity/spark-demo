package com.person.sbt.demo.utils

import org.apache.spark.sql.types.{DataType, StructType}

/**
 * @ClassName JsonUtils
 * @author apophis
 * @date 2022/8/23
 * @desc 工程介绍
 */
object JsonUtils {
  private val sample: String =
    """
      |
      |{
      |	"key": "LGWEF6A55MH000060",
      |	"value": "{\"series\":\"B03\",\"model\":\"哈弗B03_CC6470BK21A_基础款-黑色内饰_顶配\",\"brand\":\"哈弗\",\"device_id\":\"LGWEF6A55MH000060\",\"vin_trip_id\":\"LGWEF6A55MH000060_1661233834000_1661233845053\",\"bean_trip_id\":\"LGWEF6A55MH000060_1661233834000_1661233845053\",\"item_time\":1661237199000,\"item_datetime\":\"2022-08-23 14:46:39.000\",\"process_time\":1661237211564,\"process_datetime\":\"2022-08-23 14:46:51.564\",\"command\":\"STATUS\",\"tbox_protocol_type\":\"T5\",\"bean_id\":\"2797131935815204864\",\"source_kafka_header\":{\"kfk_topic\":\"tbox_original_data_t5\",\"kfk_key\":\"LGWEF6A55MH000060\",\"kfk_partition\":2,\"kfk_offset\":60595753,\"kfk_timestamp\":1661237211433,\"kfk_datetime\":\"2022-08-23 14:46:51.433\"},\"code_map\":{\"2101001\":\"234.6975\",\"2101002\":\"240.1875\",\"2017002\":\"15\",\"2101003\":\"233.325\",\"2101004\":\"222.345\",\"2016001\":\"2\",\"2101005\":\"35\",\"2101006\":\"44\",\"2101007\":\"33\",\"4105019\":\"0\",\"2101008\":\"34\",\"2059001\":\"4\",\"4105017\":\"38.831507\",\"2202001\":\"1\",\"4105018\":\"0\",\"4105016\":\"115.450179\",\"2012001\":\"2\",\"2011001\":\"24044\",\"2013001\":\"13.896484375\",\"2011003\":\"0\",\"2011002\":\"0\",\"2013004\":\"52\",\"2013016\":\"98\",\"2013005\":\"74\"},\"model_code\":\"CC6470BK21A\",\"energy_type\":\"2\",\"trip_msg\":{\"watermark\":1661237209633,\"merge_time\":1661237211633,\"action\":\"START\",\"last_time\":1661237198000,\"pre_start_code\":\"2012001\",\"pre_start\":1661233834000}}",
      |	"partition": 0,
      |	"offset": 109865758,
      |	"kafkaTime": 1661237216000
      |}
      |
      |""".stripMargin

  def getSchema(): StructType = {
    val schemaFromJson: StructType = DataType.fromJson(sample).asInstanceOf[StructType]
    schemaFromJson
  }
}
