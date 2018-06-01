package com.microsoft.jobs.OrderCharges

import java.util.{Properties, UUID}
import com.microsoft.common.PushToSQL
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
import java.util

import com.microsoft.jobs.offers.OffersTable.getAzureFiles
import org.apache.spark.sql._

import scala._
import scala.collection.immutable.Map
import scala.collection.immutable.StringOps
import scala.collection.mutable
import scala.collection.mutable.ListBuffer
//import scala.reflect.ClassTag$
import scala.runtime._


object OrderCharges {
  var spark: SparkSession = _
  val configReader = new ConfigReader("OrderCharges.conf")
  var inputArgsMap: Map[String, String] = _
  val sqlQueries = new Properties
  private val log = Logger.getLogger(getClass().getName())
  val warehouseLocation = new File("spark-warehouse").getAbsolutePath
  val TABLE_ORDER_CHARGE_FACT = "OrderChargeFact"
  val MAX_CHARGE_DATE = "MAX_CHARGE_DATE"
  val MAX_CHARGE_DATE_PATTERN = "yyyy-MM-dd"
  var maxChargeDate = "9999-09-09"
  var readPathFromConfig = false

  /**
    * This function reads the configuration from
    */
  def setup(args: Array[String]) : Unit = {

    spark = SparkSession.builder
      .appName("OrderChargeDelta")
      //.master("local[*]")
      .config("spark.sql.warehouse.dir", warehouseLocation)
      .enableHiveSupport()
      .getOrCreate()

    //spark.sparkContext.hadoopConfiguration.set(configReader.getValueForKey("orderCharges.ingest.azure.fileSystemTypeKey").toString,
      //configReader.getValueForKey("orderCharges.ingest.azure.fileSystemTypeVal").toString)
    //
   // spark.sparkContext.hadoopConfiguration.set(configReader.getValueForKey("orderCharges.ingest.azure.accountKeyKey").toString,
     // configReader.getValueForKey("orderCharges.ingest.azure.accountKeyVal").toString())



    spark.conf.set("hive.exec.dynamic.partition.mode", "nonstrict")
    spark.conf.set("hive.mapred.supports.subdirectories", "true")
    spark.conf.set("mapred.input.dir.recursive", "true")
    spark.conf.set("hive.merge.mapfiles", "true")
    spark.conf.set("hive.merge.mapredfiles", "true")
    spark.conf.set("hive.merge.tezfiles", "true")

    spark.sql("ADD JAR wasb://jsonserde@azuredevopsdevrawstore1.blob.core.windows.net/json/json-serde-1.3.9.jar")
    sqlQueries.load(this.getClass.getClassLoader.getResourceAsStream("OrderChargeSQLQueries.properties"))

    // Input sanitizations
    inputArgsMap = VNextUtil.getInputParametersAsKeyValue(args)
    readPathFromConfig = VNextUtil.isReadPathsFromConfig(inputArgsMap)
    // Update maxChargeDate if passed as command line parameter.
    if (! readPathFromConfig) {
      if (inputArgsMap.get("maxChargeDate") != null && inputArgsMap.get("maxChargeDate") != None) {
        maxChargeDate = inputArgsMap.get("maxChargeDate").get
        val error = validateDateFormat(maxChargeDate, MAX_CHARGE_DATE_PATTERN)
        if (! error.isEmpty) {
          log.error(error)
          System.exit(-1)
        }
      }
    }
    log.info("******* maxChargeDate : " + maxChargeDate)


  }



  private def charge()={
    log.error("Following arguments are mandatory maxChargeDate=<Max-Charge-Date> container=<container name> account=<account name> groupSize=<batch size of files to read>")
  }
  def main(args: Array[String]): Unit = {

    if (args.length < 1) {
      charge()
      System.exit(-1)
    }

    val startTime = System.nanoTime
    try {
      setup(args)
      preliminarySteps()
      recreateHiveTable("Charges")
      recreateHiveTable("OrderChargeFull")
      recreateHiveTable("OrderCharge")
      recreateHiveTable("ChargeFact",true)
      //executeHiveQuery("MergeOrderChargeFact")
    }
    catch {
      case e: Exception => {
        e.printStackTrace()
        throw e
      }
    }

    val elapsed = (System.nanoTime - startTime) / 1e9d
    log.info("Completed. Elapsed Time : " + elapsed + " seconds")
  }
  private def preliminarySteps() : Unit = {
    val cosmosOrderUsageFactDeltaDF = DataFrameUtil.readDataFrameFromBlobAsTSV(configReader, "orderCharges.ingest.tables.OrderCharges", "cosmosOrderCharges", inputArgsMap, spark)


    println("******* cosmosOrderUsageFactDeltaDF.count() : " + cosmosOrderUsageFactDeltaDF.count())
    cosmosOrderUsageFactDeltaDF.printSchema()
    saveAsHiveTable(cosmosOrderUsageFactDeltaDF, "Cosmos_Orders_Charges_Delta")


  }
  /**
    * Recreates a Hive table after dropping it using configured SQL.
    * @param hiveTableName
    */
  def recreateHiveTable(hiveTableName : String,pushToSQL: Boolean=false): Unit = {
    val df = executeHiveQuery("hiveQuery" + hiveTableName)
    if(pushToSQL==true) {
      PushToSQL.DataPullFromDF(df, "OrderChargeFact", "O", spark)
    }
    println("******* hiveQuery" + hiveTableName + df.count())
    saveAsHiveTable(df, hiveTableName)
  }

  /**
    *  To execute a Hive query. It replaces MAX_USAGE_DATE first.
    * @param queryName
    * @return
    */
  def executeHiveQuery(queryName : String) : DataFrame = {
    val query = sqlQueries.getProperty(queryName).replaceAllLiterally(MAX_CHARGE_DATE, maxChargeDate)
    log.info("******* " + queryName + " : " + query)
    val df = spark.sql(query)
    /*
        val countMsg = "******* " + queryName + " result count : " + df.count()
        log.info(countMsg)
        println(countMsg)
        df.printSchema()
        df.show()
    */

    df
  }

  /**
    * Saves a dataframe as the specified Hive table.
    * @param df
    * @param hiveTableName
    */
  def saveAsHiveTable(df: DataFrame, hiveTableName : String): Unit = {
    if(df != null) {
      val dropQuery = getDropQuery(hiveTableName)
      log.info("******* dropQuery  : " + dropQuery)
      spark.sql(dropQuery)
      df.write.mode(SaveMode.Overwrite).saveAsTable(hiveTableName)
      log.info("******* Data written to the (recreated) table " + hiveTableName)
    }
  }

  /**
    * Inserts a dataframe data to an existing Hive table.
    * @param df
    * @param hiveTableName
    */
  def insertIntoHiveTable(df: DataFrame, hiveTableName : String): Unit = {
    if(df != null && df.count() > 0) {
      val tmpTable = "tmp" + hiveTableName
      df.createOrReplaceTempView(tmpTable)
      val insertTableQuery = "insert into table " + hiveTableName + " select * from " + tmpTable
      log.info("******* insertTableQuery  : " + insertTableQuery)
      spark.sql(insertTableQuery)
    }
  }

  def getDropQuery(hiveTableName : String) : String = {
    "DROP TABLE IF EXISTS " + hiveTableName
  }

  /**
    * Validate date format based upon the specified date-pattern, e.g., MM/dd/yyyy or yyyy-MM-dd
    * @param inputDate
    * @param datePattern
    * @return
    */
  def validateDateFormat(inputDate : String, datePattern : String) : String = {
    var err = ""
    try {
      import org.joda.time.format._
      val fmt = DateTimeFormat forPattern datePattern
      val output = fmt parseDateTime inputDate
    } catch {
      case i: IllegalArgumentException => {
        err = "Invalid date format : " + inputDate
      }
      case e: Exception => {
        err = "Invalid date format : (" + inputDate + "). The error is : " + e.getMessage
      }
    }
    err
  }

}
