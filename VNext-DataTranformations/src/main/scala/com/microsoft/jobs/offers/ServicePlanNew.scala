package com.microsoft.jobs.offers

import java.util.Properties

import com.microsoft.common.DataFrameUtil
import com.microsoft.config.ConfigReader
import com.microsoft.framework.VNextUtil
import org.apache.spark.sql.functions.{lit, _}
import org.apache.spark.sql.types._
import org.apache.spark.sql.{Column, DataFrame, Row, SparkSession}

import scala.collection.mutable.{ListBuffer, WrappedArray}

object ServicePlanNew {


  var spark: SparkSession = null

  var jsonFiles: ListBuffer[String] = null

  val configReader = new ConfigReader("processOffers.conf")

  var inputArgsMap: Map[String, String] = null

  def setup() = {

    val configProperties = new Properties
    val inputStream = this.getClass.getClassLoader.getResourceAsStream("global.properties")
    configProperties.load(inputStream)

    spark = SparkSession.builder
      .getOrCreate()
  }



  def main(args: Array[String]): Unit = {

    if (args.length < 1) {
      print("Usage : Application requires minimum 1 argument")
      System.exit(-1)
    }

    val startTime = System.nanoTime
    setup()

    try {

      inputArgsMap = VNextUtil.getInputParametersAsKeyValue(args)
      inputArgsMap.foreach(x => println(x._1 + " " + x._2))

      val inputPath = inputArgsMap.get("input").get

      print("input path is " + inputPath)

      val readPathFromConfig = VNextUtil.isReadPathsFromConfig(inputArgsMap)

      val dfJsonInput = spark.read.json(inputPath)

      val dfOffers = dfJsonInput.select(explode(dfJsonInput("offers")))

      getServicePlansByMarket(dfOffers, readPathFromConfig)

    }
    catch {
      case e: Exception => {
        e.printStackTrace()
      }
    }
    val elapsed = (System.nanoTime - startTime) / 1e9d
    println("Done. Elapsed Time : " + elapsed + " seconds")

  }

  /**
    * Extracts ServicePlansByMarket from JSON data file.
    */
  def getServicePlansByMarket(dfOffers: DataFrame, readFromConfig: Boolean) = {

    val dfSpmData = dfOffers.select("col.ServicePlansByMarket", "col.Version", "col.OfferId", "col.PublisherId", "col.ServiceNaturalIdentifier",
      "col.PublisherNaturalIdentifier")

    val dfSpmDetailedData = dfSpmData.select("ServicePlansByMarket.*", "Version", "OfferId", "PublisherId", "ServiceNaturalIdentifier", "PublisherNaturalIdentifier")

    val dfSpmColumns = dfSpmData.select("ServicePlansByMarket.*")

    var finalDF: DataFrame = null

    val convertConditionUDF = udf(convertConditionsToStr)

    dfSpmColumns.columns.foreach(col => {

      val dfSpm = dfSpmDetailedData.select(explode(dfSpmDetailedData(col)).as("SPM_INFO"),
        dfSpmDetailedData.col("Version"), dfSpmDetailedData.col("OfferId"),
        dfSpmDetailedData.col("PublisherId"), dfSpmDetailedData.col("ServiceNaturalIdentifier"),
        dfSpmDetailedData.col("PublisherNaturalIdentifier"))

      val dfNew = dfSpm.select(
        dfSpm.col("Version"), dfSpm.col("OfferId"),
        dfSpm.col("PublisherId"), dfSpm.col("PublisherNaturalIdentifier"), dfSpm.col("ServiceNaturalIdentifier"),
        dfSpm.col("SPM_INFO.CurrencyCode"), dfSpm.col("SPM_INFO.NaturalIdentifier"), explode(dfSpm("SPM_INFO.PricePlan")).as("PP"))

      val dfNew2 = dfNew.select(dfNew.col("Version"),
        dfNew.col("OfferId"),
        dfNew.col("PublisherId"),
        dfNew.col("PublisherNaturalIdentifier").as("PublisherNID"),
        dfNew.col("ServiceNaturalIdentifier").as("ServiceNID"),
        dfNew.col("NaturalIdentifier").as("ServicePlanNID"), // ServicePlanNaturalIdentifier -- to be done by Ajmal
        dfNew.col("CurrencyCode"),
        dfNew.col("PP.Amount").as("PP_Amount"),
        dfNew.col("PP.AppliesTo").as("PP_AppliesTo"),
        dfNew.col("PP.Effect").as("PP_Effect")) //, explode(dfNew.col("PP.Conditions")).as("Conditions"))
        .withColumn("Country", lit(col))

      if (finalDF == null)
        finalDF = dfNew2
      else
        finalDF = DataFrameUtil.union(finalDF, dfNew2)

    })

    writeTableData("latestServicePlans", finalDF, inputArgsMap, readFromConfig)

  }
  def printDF(df: DataFrame, dfName: String): Unit = {
    println("*** " + dfName + " count : " + df.count())
    df.show(df.count().toInt)
  }

  def writeTableData(tableName: String, df: DataFrame, inputArgsMap: Map[String, String], readPathFromConfig: Boolean): Unit = {
    var path = if (readPathFromConfig == false)
      inputArgsMap.get(tableName).get
    else
      configReader.getValueForKey("offersDim.ingest.tables." + tableName + ".path").toString
    df.write.mode("append").json(path)
  }

  def convertConditionsToStr = (row: Row) => {
    row match {
      case null => null
      case _ => row.getString(0) + "|" + row.getBoolean(1) + "|" + row.getString(2) + "|" + row.getString(3) + "|" + row.getString(4)
    }
  }

  def checkSpaceAndConcat = (arr: WrappedArray[String]) => {

    if (arr != null) {
      arr.length match {
        case 0 => null
        case _ => {
          val arr2 = arr.map(x => x.trim).filter(_.length > 0)
          arr2.mkString("|")
        }
      }
    } else {
      null
    }
  }
}