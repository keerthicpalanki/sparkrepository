package com.microsoft.jobs.sliceindex

import java.sql.{DriverManager, ResultSet, Timestamp}
import java.text.SimpleDateFormat
import java.util.Calendar

import org.apache.log4j.Logger
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import com.microsoft.common.PushToSQL
import com.microsoft.config.ConfigReader
import com.microsoft.framework.VNextUtil

object SliceIndexUtilityBlob {
  var spark: SparkSession = _
  var log = Logger.getLogger(getClass.getName)
  val sqlTableName = "SliceIndex"

  def main(args: Array[String]): Unit = {
    if (args.length < 1) {
      log.error("Application requires minimum 1 argument")
      System.exit(-1)
    }

    spark = SparkSession.builder.appName("SliceIndexUtilityBlob").master("yarn").getOrCreate
    val inputArgsMap = VNextUtil.getInputParametersAsKeyValue(args)

    //Read arguments
    val streamName = inputArgsMap("StreamName");
    val slice = inputArgsMap("Slice");
    val slicePath = inputArgsMap("SlicePath");
    val notes = inputArgsMap("Notes");
    val runId = inputArgsMap("RunId");
    var format = inputArgsMap("Format");

    //Read options
    var options = collection.mutable.Map[String, String]()
    if (format == "tsv") {
      format = "csv";
      options += ("sep" -> "\t")
    }

    //Get count
    var recordCount: Long = 0;
    log.info(s"Creating data frame")
    var dataFrame = spark.read.format(format).options(options).load(slicePath)
    if (streamName.contains("Offers")) {
      log.info(s"Creating data frame for offers")
      dataFrame = dataFrame.select(explode(col("offers")))
    }
    recordCount = dataFrame.count;
    log.info(s"Record count : $recordCount")

    log.info(s"****************Sp Calling getETLSteps for stream : $streamName")
    val (etlid, stepid) = getETLSteps(streamName)
    log.info(s"****************Sp Call completed for stream : $streamName, args : ETLID- $etlid, StepID - $stepid")

    val sliceIndexDataFrame = getDataFrameForSliceIndex(recordCount, streamName, slice, slicePath,
      getCurrentdateTimeStamp.toString, runId, notes, etlid, stepid)
    PushToSQL.DataPullFromDF(sliceIndexDataFrame, sqlTableName, "A", spark)
  }

  def getCurrentdateTimeStamp: Timestamp = {
    val today: java.util.Date = Calendar.getInstance.getTime
    val timeFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
    val now: String = timeFormat.format(today)
    val re = java.sql.Timestamp.valueOf(now)
    re
  }

  def getDataFrameForSliceIndex(recordCount: Long, streamName: String, slice: String, slicePath: String,
                                publishTime: String, runId: String, notes: String, etlid: Integer, stepid: Integer): DataFrame = {
    val schema = new StructType()
      .add(StructField("RecordCount", LongType, true))
      .add(StructField("StreamName", StringType, true))
      .add(StructField("Slice", StringType, true))
      .add(StructField("SlicePath", StringType, true))
      .add(StructField("PublishTime", StringType, true))
      .add(StructField("RunId", StringType, true))
      .add(StructField("Notes", StringType, true))
      .add(StructField("ETLDID", IntegerType, true))
      .add(StructField("StepID", IntegerType, true))
    //      .add(StructField("DataLineageID", IntegerType, true))
    //      .add(StructField("SliceID", IntegerType, true))

    spark.createDataFrame(spark.sparkContext.parallelize(Seq(Row(recordCount, streamName, slice, slicePath,
      publishTime, runId, notes, etlid, stepid))), schema)
  }

  def getETLSteps(streamName: String): (Integer, Integer) = {
    Class.forName("com.microsoft.sqlserver.jdbc.SQLServerDriver")
    val pipelineName = streamName.split('/')(0);
    var etlid = 0
    var stepid = 0
    val configReader = new ConfigReader("SQLServer.conf")
    val jdbcSqlConnStr = configReader.getValueForKey("ingest.PasSSQL.SqlConnStr").toString
    val conn = DriverManager.getConnection(jdbcSqlConnStr)

    try {
      val cmd = conn.prepareCall("{call sp_GetETLSteps(?)}")

      cmd.setString("PipelineName", pipelineName)
      cmd.execute()
      val rs = cmd.getResultSet

      if (rs.next) {
        etlid = rs.getInt("ETLID")
        stepid = rs.getInt("StepID")
      }

    } catch {
      case e: Throwable => e.printStackTrace()
    } finally {
      conn.close()
    }
    (etlid, stepid)
  }
}
