package com.thunderhemu.com

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

object DataCopyApp extends App {
  val conf = new SparkConf().setAppName("code-challenge").setMaster("local")
  val spark = SparkSession.builder.config(conf).enableHiveSupport().getOrCreate()
  new DataCopy(spark).process(this.getClass().getClassLoader().getResourceAsStream("config.yaml"),args)
  spark.stop()
}
