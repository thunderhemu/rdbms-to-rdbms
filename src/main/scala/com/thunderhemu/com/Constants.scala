package com.thunderhemu.com

import java.io.InputStream

import org.yaml.snakeyaml.Yaml

class Constants(input : InputStream) {

  private val yaml = new Yaml()
  private  var obj = new java.util.LinkedHashMap[String,String]
  obj = yaml.load(input)

  val SOURCE_TABLE_NAME = obj.getOrDefault("source_table_name",null)
  val SOURCE_DRIVER = obj.getOrDefault("source_driver",null)
  val SOURCE_USER =    obj.getOrDefault("source_user",null)
  val SOURCE_PASSWORD =  obj.getOrDefault("source_password",null)
  val SOURCE_TABLE_TO_COLUMNS_SELECT =  obj.getOrDefault("source_columns",null)
  val SOURCE_JDBC_URL =  obj.getOrDefault("source_jdbc",null)
  val TARGET_TABLE_NAME = obj.getOrDefault("target_table_name",null)
  val TARGET_DRIVER = obj.getOrDefault("target_driver",null)
  val TARGET_USER =    obj.getOrDefault("target_user",null)
  val TARGET_PASSWORD =  obj.getOrDefault("target_password",null)
  val TARGET_TABLE_TO_COLUMNS_SELECT =  obj.getOrDefault("target_columns",null)
  val TARGET_JDBC_URL =  obj.getOrDefault("target_jdbc",null)
  val TARGET = obj.getOrDefault("target",null)
  val OVERLOADING_FLAG =  obj.getOrDefault("overloading_table",null)
  val SOURCE_TABLE_CONDITION_COLUMN = obj.getOrDefault("source_table_where_condition_column",null)
  val DATA_SELECTION_RULE = obj.getOrDefault("sourcer_condition_column_type",null)

}
