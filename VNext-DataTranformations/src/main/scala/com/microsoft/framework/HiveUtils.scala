package com.microsoft.framework

import java.util.Properties

import com.microsoft.common.DataFrameUtil
import org.apache.log4j.Logger
import org.apache.spark
import org.apache.spark.sql.SaveMode._
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}

object HiveUtils {

  private val log = Logger.getLogger(getClass().getName())

  def recreateHiveTable(hiveTableName : String, spark: SparkSession, sqlQuery : String, dbName : String): DataFrame = {
    val df = executeHiveQuery(sqlQuery, spark, dbName)
    saveAsHiveTable(df, hiveTableName,spark, dbName)
    df
  }

  /**
    *  To execute a Hive query. It replaces MAX_USAGE_DATE first.
    * @param queryName
    * @return
    */
  def executeHiveQuery(query : String, spark: SparkSession, dbName : String) : DataFrame = {
    spark.sql(" USE  " + dbName)
    log.info("*******  query is  : " + query)
    val df = spark.sql(query)
    df
  }


  def getDropQuery(schema: String, hiveTableName : String) : String = {
    "DROP TABLE IF EXISTS " + schema + "." + hiveTableName
  }

  /**
    * Saves a dataframe as the specified Hive table.
    *
    * @param df
    * @param hiveTableName
    */
  def saveAsHiveTable(df: DataFrame, hiveTableName : String,spark: SparkSession, dbName : String): Unit = {
    if(df != null) {
      val dropQuery = getDropQuery(dbName, hiveTableName)
      log.info("******* dropQuery  : " + dropQuery)
      spark.sql(dropQuery)
      df.write.mode(Overwrite).saveAsTable(dbName + "." + hiveTableName)
    }
  }

  def selectDataFromTable(hiveTableName : String,spark: SparkSession,  inputArgsMap : Map[String, String], props : Properties): DataFrame = {

      val createTableQuery = "CREATE TABLE IF NOT EXISTS " + inputArgsMap.get("dbName").get + "." + hiveTableName +
                        " USING JSON "  +
                        " LOCATION "  + inputArgsMap.get(hiveTableName).get
      spark.sql(createTableQuery)

      val selectQuery = props.getProperty(hiveTableName)

      spark.sql(selectQuery)

  }

  def createAndGetDataFromTable(hiveTableName : String,spark: SparkSession,  inputArgsMap : Map[String, String], props : Properties): DataFrame = {

    val selectQuery = props.getProperty(hiveTableName)

    spark.sql(selectQuery)

  }



}
