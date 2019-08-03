package com.thunderhemu.com.tests

import java.io.{File, FileInputStream, InputStream}

import com.thunderhemu.com.{Constants, DataCopy}
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.scalatest.{BeforeAndAfter, BeforeAndAfterAll, FunSuite, Matchers}
import org.scalatest.concurrent.Eventually
import scalikejdbc.{AutoSession, ConnectionPool}
import scalikejdbc._

class DataCopyTest extends FunSuite with BeforeAndAfterAll with BeforeAndAfter with Matchers with Eventually {

  val conf = new SparkConf().setAppName("code-challenge-test").setMaster("local[*]")
  val spark = SparkSession.builder.config(conf).enableHiveSupport().getOrCreate()
  val warehouserDir = new File("src/test/resources/output")


  override def beforeAll(): Unit = {
    Class.forName("org.h2.Driver")
    ConnectionPool.singleton("jdbc:h2:mem:test", "user", "pass")
    implicit val session = AutoSession
    super.beforeAll()
    // create tables
    sql"""
           create table ACCOUNT_SOURCE (
                                  accountid varchar(64) not null primary key,
                                  name varchar(64) )""".execute.apply()

    sql"insert into ACCOUNT_SOURCE (accountid,name) values ('A','Company A')".update.apply()
    sql"insert into ACCOUNT_SOURCE (accountid,name) values ('B','Company B')".update.apply()

    sql"""
           create table ACCOUNT_TARGET (
                                  accountid varchar(64) not null primary key,
                                  name varchar(64) )""".execute.apply()
  }

  override def afterAll() = {
    spark.stop
  }
  def deleteRecursively(file: File): Unit = {
    if (file.isDirectory)
      file.listFiles.foreach(deleteRecursively)
    if (file.exists && !file.delete)
      throw new Exception(s"Unable to delete ${file.getAbsolutePath}")
  }

  test("Invalid sourcer driver") {
    val thrown = intercept[Exception]{
      val input  : InputStream = new FileInputStream(new File("src/test/resources/input/config/invalid-source-driver.yaml"))
      val constants = new Constants(input)
      new DataCopy(spark).validation(constants)
    }
    assert(thrown.getMessage == "requirement failed: Invalid source_driver, kindly provide valid driver")
  }

  test("Invalid sourcer jdbc") {
    val thrown = intercept[Exception]{
      val input  : InputStream = new FileInputStream(new File("src/test/resources/input/config/invalid-source-jdbc.yaml"))
      val constants = new Constants(input)
      new DataCopy(spark).validation(constants)
    }
    assert(thrown.getMessage == "requirement failed: Invalid source_jdbcurl, kindly provide valid jdbcurl")
  }

  test("Invalid sourcer user") {
    val thrown = intercept[Exception]{
      val input  : InputStream = new FileInputStream(new File("src/test/resources/input/config/invalid-source-user.yaml"))
      val constants = new Constants(input)
      new DataCopy(spark).validation(constants)
    }
    assert(thrown.getMessage == "requirement failed: Invalid source_user id, kindly provide valid user id")
  }

  test("Invalid sourcer password") {
    val thrown = intercept[Exception]{
      val input  : InputStream = new FileInputStream(new File("src/test/resources/input/config/invalid-source-password.yaml"))
      val constants = new Constants(input)
      new DataCopy(spark).validation(constants)
    }
    assert(thrown.getMessage == "requirement failed: Invalid source_password, kindly provide valid password")
  }

  test("Invalid target driver") {
    val thrown = intercept[Exception]{
      val input  : InputStream = new FileInputStream(new File("src/test/resources/input/config/invalid-target-driver.yaml"))
      val constants = new Constants(input)
      new DataCopy(spark).validation(constants)
    }
    assert(thrown.getMessage == "requirement failed: Invalid Target Driver, target Driver shouldn't be null")
  }

  test("Invalid target jdbc") {
    val thrown = intercept[Exception]{
      val input  : InputStream = new FileInputStream(new File("src/test/resources/input/config/invalid-target-jdbc.yaml"))
      val constants = new Constants(input)
      new DataCopy(spark).validation(constants)
    }
    assert(thrown.getMessage == "requirement failed: Invalid Target_jdbcurl, kindly provide valid jdbcurl")
  }

  test("Invalid target user") {
    val thrown = intercept[Exception]{
      val input  : InputStream = new FileInputStream(new File("src/test/resources/input/config/invalid-target-user.yaml"))
      val constants = new Constants(input)
      new DataCopy(spark).validation(constants)
    }
    assert(thrown.getMessage == "requirement failed: Invalid  Target user, kindly provide valid user id")
  }

  test("Invalid target password") {
    val thrown = intercept[Exception]{
      val input  : InputStream = new FileInputStream(new File("src/test/resources/input/config/invalid-target-password.yaml"))
      val constants = new Constants(input)
      new DataCopy(spark).validation(constants)
    }
    assert(thrown.getMessage == "requirement failed: Invalid Target_password, kindly provide valid password")
  }

  test("Invalid target flag") {
    val thrown = intercept[Exception]{
      val input  : InputStream = new FileInputStream(new File("src/test/resources/input/config/invalid-target-flag.yaml"))
      val constants = new Constants(input)
      new DataCopy(spark).validation(constants)
    }
    assert(thrown.getMessage == "requirement failed: Invalid target, target shouldn't be null")
  }

  test("Invalid overwrite flag") {
    val thrown = intercept[Exception]{
      val input  : InputStream = new FileInputStream(new File("src/test/resources/input/config/invalid-overwrite-flag.yaml"))
      val constants = new Constants(input)
      new DataCopy(spark).validation(constants)
    }
    assert(thrown.getMessage == "requirement failed: Invalid overloading flag, kindly provide valid flag  yes to overwrite, no to append data")
  }

  test("full load append"){
    val input  : InputStream = new FileInputStream(new File("src/test/resources/input/config/test1.yaml"))
   new DataCopy(spark).process(input, Array(""))
    val df = spark.sqlContext.read.format("jdbc")
      .option("url", "jdbc:h2:mem:test")
      .option("driver", "org.h2.Driver")
      .option("dbtable", "( select * from  ACCOUNT_TARGET )")
      .option("user", "user")
      .option("password", "pass")
      .load()
    assert(df.count() > 1)
  }

  test("continous load append"){
    val input  : InputStream = new FileInputStream(new File("src/test/resources/input/config/test2.yaml"))
    new DataCopy(spark).process(input, Array("A"))
    val df = spark.sqlContext.read.format("jdbc")
      .option("url", "jdbc:h2:mem:test")
      .option("driver", "org.h2.Driver")
      .option("dbtable", "( select * from  ACCOUNT_TARGET )")
      .option("user", "user")
      .option("password", "pass")
      .load()
    assert(df.count() == 1)
  }

}
