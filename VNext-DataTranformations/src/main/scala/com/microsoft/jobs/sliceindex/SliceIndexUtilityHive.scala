package com.microsoft.jobs.sliceindex

import java.sql.Timestamp
import java.text.SimpleDateFormat
import java.util.Calendar

import com.microsoft.common.PushToSQL
import org.apache.log4j.Logger
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions.lit

object SliceIndexUtilityHive {
  var log = Logger.getLogger(getClass.getName)

  def sliceIndex(slice: String, runId: String, spark: SparkSession, query: String, streamName: String): Unit = {
    log.error(s"SliceIndex logging for streamname: $streamName, slcie: $slice, query: $query")
    val startTime = System.nanoTime
    val sqlTableName = "SliceIndex"
    try {
      var sliceIndexDF = spark.sql(query)

      sliceIndexDF = sliceIndexDF.withColumn("StreamName", lit(streamName))
      sliceIndexDF = sliceIndexDF.withColumn("Slice", lit(slice))
      sliceIndexDF = sliceIndexDF.withColumn("SlicePath", lit("Hive"))
      sliceIndexDF = sliceIndexDF.withColumn("PublishTime", lit(getCurrentdateTimeStamp))
      sliceIndexDF = sliceIndexDF.withColumn("RunId", lit(runId))
      sliceIndexDF = sliceIndexDF.withColumn("Notes", lit(""))
      sliceIndexDF = sliceIndexDF.withColumn("ETLDID", lit(0))
      sliceIndexDF = sliceIndexDF.withColumn("StepID", lit(0))
      //      sliceIndexDF = sliceIndexDF.withColumn("DataLineageID", lit(0))
      //      sliceIndexDF = sliceIndexDF.withColumn("SliceID", lit(0))

      //var pushToSQL = true
      //      if (inputArgsMap.get("pushToSQL") != null && inputArgsMap.get("pushToSQL").get.equalsIgnoreCase("false")) {
      //        pushToSQL = false
      //      }

      //if (pushToSQL && sqlTableName != null) {
      log.error("Started Writing to sql server table " + sqlTableName)
      PushToSQL.DataPullFromDF(sliceIndexDF, sqlTableName, "A", spark)
      log.error("Finished Writing to sql server table " + sqlTableName)
      //}
    } catch {
      case e: Exception => {
        e.printStackTrace()
        throw e
      }
    }

    val elapsed = (System.nanoTime - startTime) / 1e9d
    log.info("Completed. Elapsed Time : " + elapsed + " seconds")
  }

  def getCurrentdateTimeStamp: Timestamp = {
    val today: java.util.Date = Calendar.getInstance.getTime
    val timeFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
    val now: String = timeFormat.format(today)
    val re = java.sql.Timestamp.valueOf(now)
    re
  }
}
