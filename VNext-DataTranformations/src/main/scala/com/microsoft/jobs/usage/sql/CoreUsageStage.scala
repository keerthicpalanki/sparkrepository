package com.microsoft.jobs.usage.sql

import java.util.{Calendar, Properties, UUID}

import com.microsoft.common.DataFrameUtil
import com.microsoft.config.ConfigReader
import com.microsoft.framework.VNextUtil
import com.microsoft.jobs.order.ProcessOrders._
import org.apache.log4j.Logger
import org.apache.spark.sql.{DataFrame, RelationalGroupedDataset, SparkSession}
import org.apache.spark.sql.functions.{when, _}
import org.apache.spark.sql.types._
import java.io.File

import com.microsoft.azure.storage.CloudStorageAccount
import com.microsoft.azure.storage.blob.CloudBlobClient
import com.microsoft.azure.storage.blob.CloudBlobContainer
import com.microsoft.azure.storage.blob.CloudBlobDirectory
import com.microsoft.azure.storage.blob.ListBlobItem
import java.io.File
import java.net.URI
import java.text.SimpleDateFormat
import java.util

import org.apache.spark.sql._

import scala._
import scala.collection.immutable.Map
import scala.collection.immutable.StringOps
import scala.collection.mutable
//import scala.reflect.ClassTag$
import scala.runtime._

object CoreUsageStage {
  var spark: SparkSession = _
  val configReader = new ConfigReader("orderUsageDeltaSql.conf")
  var inputArgsMap: Map[String, String] = _
  val sqlQueries = new Properties
  private val log = Logger.getLogger(getClass().getName())
  val warehouseLocation = new File("spark-warehouse").getAbsolutePath
  var hiveDB = "default"
  var DataLineageID = UUID.randomUUID().toString

  def setup(args: Array[String]) : Unit = {

    spark = SparkSession.builder
      //        .master("local[*]")
      .appName("CoreUsageStage")
      .config("spark.sql.warehouse.dir", warehouseLocation)
      .enableHiveSupport()
      .getOrCreate()

    spark.conf.set("hive.exec.dynamic.partition.mode", "nonstrict")
    spark.conf.set("hive.mapred.supports.subdirectories", "true")
    spark.conf.set("mapred.input.dir.recursive", "true")


    spark.sql("ADD JAR wasb://jsonserde@azuredevopsdevrawstore1.blob.core.windows.net/json/json-serde-1.3.9.jar")
    sqlQueries.load(this.getClass.getClassLoader.getResourceAsStream("sqlQueries.properties"))



  }

  def main(args: Array[String]): Unit = {

    if (args.length < 1) {
      System.exit(-1)
    }
    val startTime = System.nanoTime
    try {
      setup(args)

      recreateHiveTable("CoreUsage_Stg")


    }
    catch {
      case e: Exception => {
        e.printStackTrace()
        throw e
      }
    }
  }

  def recreateHiveTable(hiveTableName : String): Unit = {
    val df = executeHiveQuery("hiveQuery" + hiveTableName)
    saveAsHiveTable(df, hiveTableName)
  }

  def executeHiveQuery(queryName : String) : DataFrame = {
    var query = sqlQueries.getProperty(queryName)
    log.info("******* " + queryName + " : " + query)
    val df = spark.sql(query)

    df
  }

  def saveAsHiveTable(df: DataFrame, hiveTableName : String): Unit = {
    if(df != null) {
      val dropQuery = getDropQuery(hiveTableName)
      log.info("******* dropQuery  : " + dropQuery)
      spark.sql(dropQuery)
      df.write.mode(SaveMode.Overwrite).saveAsTable(hiveTableName)
      log.info("******* Data written to the (recreated) table " + hiveTableName)
    }
  }

  def getDropQuery(hiveTableName : String) : String = {
    "DROP TABLE IF EXISTS " + hiveTableName
  }
}
