package com.microsoft.common

import com.microsoft.config.ConfigReader
import org.apache.spark.sql.{DataFrame, SparkSession}

object PushToSQL {
  val configReader = new ConfigReader("SQLServer.conf")

  def DataPullFromDF(df: DataFrame, sqltableName: String, sqlSaveMode: String,spark: SparkSession) = {
    try {
      val jdbcSqlConnStr = configReader.getValueForKey("ingest.PasSSQL.SqlConnStr").toString
      val driverClass = "com.microsoft.sqlserver.jdbc.SQLServerDriver"
      val connectionProperties = new java.util.Properties()
      connectionProperties.setProperty("Driver", driverClass)
      connectionProperties.setProperty("AutoCommit", "true")

      if(sqlSaveMode=="A"){
        df.write.mode(saveMode = "Append").jdbc(jdbcSqlConnStr, sqltableName, connectionProperties)
      }
      else if(sqlSaveMode=="O"){
        df.write.mode(saveMode = "Overwrite").jdbc(jdbcSqlConnStr, sqltableName, connectionProperties)
      }
      else if(sqlSaveMode=="I"){
        df.write.mode(saveMode = "Ignore").jdbc(jdbcSqlConnStr, sqltableName, connectionProperties)
      }
      else if(sqlSaveMode=="E"){
        df.write.mode(saveMode = "ErrorIfExists").jdbc(jdbcSqlConnStr, sqltableName, connectionProperties)
      }
      else  {
        df.write.jdbc(jdbcSqlConnStr, sqltableName, connectionProperties)
      }
    } catch {

      case e: Exception => println(e.printStackTrace());
    }
  }


  def DataPullFromBlobAsJSON(configReader: ConfigReader, basePath: String, tableName:String, inputArgsMap: Map[String, String], readPathFromConfig:Boolean, sqltableName: String, sqlSaveMode: String, spark: SparkSession) = {
    try {

      val df = DataFrameUtil.readDataFrameFromBlob(configReader, basePath,tableName, inputArgsMap, spark)
      PushToSQL.DataPullFromDF(df,sqltableName,sqlSaveMode,spark)

    } catch {

      case e: Exception => println(e.printStackTrace());
    }
  }

  def DataPullFromBlobAsTSV(configReader: ConfigReader, basePath: String, tableName:String, inputArgsMap: Map[String, String], readPathFromConfig:Boolean, sqltableName: String, sqlSaveMode: String, spark: SparkSession) = {
    try {

      val df = DataFrameUtil.readDataFrameFromBlobAsTSV(configReader, basePath,tableName, inputArgsMap, spark)
      PushToSQL.DataPullFromDF(df,sqltableName,sqlSaveMode,spark)

    } catch {

      case e: Exception => println(e.printStackTrace());
    }
  }

}
