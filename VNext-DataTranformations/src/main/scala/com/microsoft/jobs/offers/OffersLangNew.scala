package com.microsoft.jobs.offers

import java.util.Properties

import com.microsoft.config.ConfigReader
import com.microsoft.framework.VNextUtil
import org.apache.spark.sql.functions.{lit, _}
import org.apache.spark.sql.types._
import org.apache.spark.sql.{Column, DataFrame, Row, SparkSession}

import scala.collection.mutable.{ListBuffer, WrappedArray}

object OffersLangNew {


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

      getOffersLanguage(dfOffers, readPathFromConfig)

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
    * Extracts OffersLanguage from JSON data file.
    */
  def getOffersLanguage(dfOffers: DataFrame, readFromConfig: Boolean) = {

    val dfLangData = dfOffers.select(
      when(col("Languages").isNull, lit(null)).otherwise(col("Languages")).as("Languages"),
      when(col("Version").isNull, lit(null).as("String")).otherwise(col("Version")).as("Version"),
      when(col("OfferId").isNull, lit(null).as("String")).otherwise(col("OfferId")).as("OfferId"),
      when(col("PublisherId").isNull, lit(null).as("String")).otherwise(col("PublisherId")).as("PublisherId"),
      when(col("ServiceNaturalIdentifier").isNull, lit(null).as("String")).otherwise(col("ServiceNaturalIdentifier")).as("ServiceNaturalIdentifier"),
      when(col("PublisherNaturalIdentifier").isNull, lit(null).as("String")).otherwise(col("PublisherNaturalIdentifier").as("PublisherNaturalIdentifier")))

    val dfLangDetailData = dfLangData.select(
      when(col("Languages").isNull, lit(null)).otherwise(col("Languages.en-US")).as("en-US"),
      col("Version"),
      col("OfferId"),
      col("PublisherId"),
      col("ServiceNaturalIdentifier"),
      col("PublisherNaturalIdentifier"))

    val dfLang = dfLangDetailData.select(
      col("Version"), col("OfferId"), col("PublisherId"), col("PublisherNaturalIdentifier"), col("ServiceNaturalIdentifier"),
      when(col("en-US").isNull, lit(null).as("String")).otherwise(col("en-US.Title")).as("Title"),
      when(col("en-US"), lit(null).as("String")).otherwise(col("en-US.Summary")),
      when(col("en-US"), lit(null).as("String")).otherwise(explode(col("en-US.ServicePlanDescriptions"))).as("SP"))

    val dfLang1 = dfLang.select(
      col("Version"), col("OfferId"), col("PublisherId"), col("PublisherNaturalIdentifier").as("PublisherNID"),
      col("ServiceNaturalIdentifier").as("ServiceNID"),
      col("Title").as("Title"),
      when(col("SP").isNull, lit(null).as("String")).otherwise(col("SP.Title")).as("SP_Title"),
      when(col("SP").isNull, lit(null).as("String")).otherwise(col("SP.Description")).as("SP_Description"),
      when(col("SP").isNull, lit(null).as("String")).otherwise(col("SP.Summary")).as("SP_Summary"),
      when(col("SP").isNull, lit(null).as("String")).otherwise(col("SP.ServicePlanNaturalIdentifier")).as("SP_ServicePlanNID"),
      when(col("SP").isNull, lit(null).as("String")).otherwise(col("SP.Features")).as("SP_Features"),
      when(col("SP").isNull, lit(null).as("String")).otherwise(col("SP.RequiresExternalLicense")).as("SP_RequiresExtLicense"),
      when(col("SP").isNull, lit(null).as("String")).otherwise(col("SP.RequiresExternalLicense")).as("SP_RequiresExtLicense"))
      .withColumn("Language", lit("en-US")).withColumn("IsExternallyBilled", when(col("SP_RequiresExtLicense") === true, 1).otherwise(0))

    val convertConditionUDF = udf(checkSpaceAndConcat)

    //val dfLang3 = dfLang1.withColumn("SP_Features", convertConditionUDF(col("SP_Features"))) Please test. Giving an error

    writeTableData("OfferDetails", dfLang1, inputArgsMap, readFromConfig)
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