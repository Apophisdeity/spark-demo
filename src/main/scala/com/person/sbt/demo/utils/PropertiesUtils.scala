package com.person.sbt.demo.utils

import java.io.FileInputStream
import java.util.Properties

/**
 * @ClassName PropertiesUtils
 * @author apophis
 * @date 2022/8/23
 * @desc 工程介绍
 */
object PropertiesUtils {
  lazy val get: Properties = {
    load("C:/Users/apophis/Desktop/conf.properties")
  }


  def load(propertiesFile: String): Properties = {
    val properties = new Properties
    properties.load(new FileInputStream(propertiesFile))
    properties
  }
}
