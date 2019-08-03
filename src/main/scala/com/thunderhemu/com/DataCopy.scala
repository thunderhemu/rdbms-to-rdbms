package com.thunderhemu.com

import java.io.InputStream

import org.apache.commons.lang3.math.NumberUtils
import org.apache.log4j.LogManager
import org.apache.spark.sql.{DataFrame, SparkSession}

class DataCopy(spark : SparkSession) {
  val log = LogManager.getRootLogger
  def process(yamlStream : InputStream,args : Array[String]) : Unit = {
    val constants = new Constants(yamlStream)
    validation(constants)
    val conditionValue : String = if ( args.length >0 ) args(0) else  ""
    val rdbmsDf = getDataFromJdbms( constants.SOURCE_JDBC_URL,
                                    constants.SOURCE_DRIVER,
                                    constants.SOURCE_USER,
                                    constants.SOURCE_PASSWORD,
                                    getSelectQuery(constants,conditionValue) )
    rdbmsDf.createGlobalTempView("temp_table")
    val writeDf = if (constants.TARGET_TABLE_TO_COLUMNS_SELECT != null )
                      spark.sql(s"""select ${constants.TARGET_TABLE_TO_COLUMNS_SELECT} from temp_table """)
                  else rdbmsDf
    writeDf.show()
    val prop = new java.util.Properties
    prop.setProperty("driver", constants.TARGET_DRIVER)
    prop.setProperty("user", constants.TARGET_USER)
    prop.setProperty("password", constants.TARGET_PASSWORD)
    writeToJdbc(writeDf,constants.TARGET_JDBC_URL,constants.TARGET_TABLE_NAME,prop)
  }

    def dataSelecttionStr( rule : String,referenceValue : String ) : String = {
      var returnStr = " "
      rule match {
        case "timestamp" => returnStr = " > to_timestamp('" +referenceValue +"','YYYY-MM-DD HH24:MI:SS.FF')"
        case "int"       => returnStr = " > " +NumberUtils.toInt(referenceValue)
        case "double"    => returnStr = " > " +NumberUtils.toDouble(referenceValue)
        case "string"    => returnStr = " > '" + referenceValue +"'"
        case "day"       => returnStr = " > trunc (to_timestamp('" +referenceValue +"','YYYY-MM-DD HH24:MI:SS.FF')) - 1/24/60/60"
        case "month"     => returnStr = " > trunc (last_day (add_months (to_timestamp('" + referenceValue +"','YYYY-MM-DD HH24:MI:SS.FF') - 1, -1)) + 1) - 1/24/60/60"
        case _           =>  log.error("invalide data selection rule the passed value is ---> " +rule) ; sys.exit(-1)
      }
      returnStr
    }

  def getSelectQuery(constants: Constants,conditionValue : String ): String = {
    val columns = if (constants.SOURCE_TABLE_TO_COLUMNS_SELECT == null ) "*"
                  else constants.SOURCE_TABLE_TO_COLUMNS_SELECT
    val filterCondition = if(constants.OVERLOADING_FLAG.toLowerCase == "true") ""
                          else " where " + constants.SOURCE_TABLE_CONDITION_COLUMN + dataSelecttionStr(constants.DATA_SELECTION_RULE,conditionValue)
    s"""select $columns from ${constants.SOURCE_TABLE_NAME} $filterCondition"""
  }

  def validation( constants : Constants ) : Unit = {
    require(constants.SOURCE_DRIVER   != null , "Invalid source_driver, kindly provide valid driver")
    require( constants.SOURCE_USER    != null ,"Invalid source_user id, kindly provide valid user id")
    require(constants.SOURCE_PASSWORD != null ,"Invalid source_password, kindly provide valid password")
    require(constants.SOURCE_JDBC_URL != null ,"Invalid source_jdbcurl, kindly provide valid jdbcurl")
    require(constants.OVERLOADING_FLAG != null ,"Invalid overloading flag, kindly provide valid flag  yes to overwrite, no to append data")
    require(constants.OVERLOADING_FLAG != null, "Invalid overwrite flag , overwrite flag can't be full")
    require(constants.OVERLOADING_FLAG.toLowerCase != "yes" |
             constants.OVERLOADING_FLAG.toLowerCase != "no","Invalid overwrite flag it should be true/False for full load / continuous load ")
    require(constants.TARGET          != null ,"Invalid target, target shouldn't be null")
    if (constants.TARGET == "jdbc" ){
      require(constants.TARGET_DRIVER != null , "Invalid Target Driver, target Driver shouldn't be null")
      require(constants.TARGET_USER   != null , "Invalid  Target user, kindly provide valid user id")
      require(constants.TARGET_PASSWORD != null ,"Invalid Target_password, kindly provide valid password")
      require(constants.TARGET_JDBC_URL != null ,"Invalid Target_jdbcurl, kindly provide valid jdbcurl")
    }
    require(constants.OVERLOADING_FLAG.toLowerCase != "yes" |
      constants.SOURCE_TABLE_CONDITION_COLUMN != null,"Invalid source_table_where_condition_column, it should be null for continuos load ")
  }

  def getDataFromJdbms( jdbcUrl : String, driver : String ,user : String , password : String ,query : String)  =
    spark.sqlContext.read.format("jdbc")
      .option("url", jdbcUrl)
      .option("driver", driver)
      .option("dbtable", "(" + query +" )")
      .option("user", user)
      .option("password", password)
      .load()

  def writeToJdbc(df : DataFrame,jdbcString : String,tableName : String, prop : java.util.Properties)  =
    df.write.mode("append").jdbc(jdbcString, tableName, prop)

  }


