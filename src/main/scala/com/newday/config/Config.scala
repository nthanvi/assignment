package com.newday.config


import org.apache.commons.configuration2.{PropertiesConfiguration}
import org.apache.commons.configuration2.builder.fluent.Configurations

object Config {
  private var properties:Option[PropertiesConfiguration] = None
  def getProperty(name:String, defaultValue:String):String= properties.get.getString(name, defaultValue);
  def init(path:String) {
      if (!properties.isDefined) {
        properties = Some(new Configurations().properties(path))
      }
  }
}



