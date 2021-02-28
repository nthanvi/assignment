package com.newday.config


import org.apache.commons.configuration2.{ConfigurationMap, PropertiesConfiguration}
import org.apache.commons.configuration2.builder.fluent.Configurations

object Config {
  private val properties = new Configurations().properties(this.getClass.getResource("/env.properties"))
  def getProperty(name:String, defaultValue:String):String= properties.getString(name, defaultValue);
}



