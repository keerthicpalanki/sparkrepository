package com.microsoft.jobs

import java.util.Properties

import com.microsoft.common.DataFrameUtil
import com.microsoft.config.ConfigReader
import com.microsoft.framework.VNextUtil
import org.apache.log4j.Logger
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.StringType

object Enrollment {

  var spark: SparkSession = null
  val configReader = new ConfigReader("enrollment.conf")
  var inputArgsMap: Map[String, String] = null

  val log = Logger.getLogger(getClass().getName())

  /**
    * This function reads the configuration from
    */
  def setup() = {


    spark = SparkSession.builder

      .getOrCreate()


    spark.conf.set("spark.sql.crossJoin.enabled", "true")
  }


  def main(args: Array[String]): Unit = {
    if (args.length < 1) {
      log.error("Usage : Application requires minimum 1 argument")
      System.exit(-1)
    }
    val startTime = System.nanoTime
    inputArgsMap = VNextUtil.getInputParametersAsKeyValue(args)
    setup()

    val readPathFromConfig = VNextUtil.isReadPathsFromConfig(inputArgsMap)

    val enrollmentDF = DataFrameUtil.readDataFrameFromBlobAsTSV(configReader, "enrollmentDim.ingest.tables.enrollments", "enrollments", inputArgsMap, spark)

    val statusDF = DataFrameUtil.readDataFrameFromBlobAsTSV(configReader, "enrollmentDim.ingest.tables.status", "status", inputArgsMap, spark)
    val externalStatusDF = statusDF.select("*")
    //val externalStatusDF = DataFrameUtil.readDataFrameFromBlobAsTSV(configReader, "enrollmentDim.ingest.tables.externalStatus", "externalStatus", inputArgsMap,  spark)
    val operationsCenters = DataFrameUtil.readDataFrameFromBlobAsTSV(configReader, "enrollmentDim.ingest.tables.operationsCenters", "operationsCenters", inputArgsMap,  spark)
    val regions = DataFrameUtil.readDataFrameFromBlobAsTSV(configReader, "enrollmentDim.ingest.tables.regions", "regions", inputArgsMap, spark)
    val enrollmentPrograms = DataFrameUtil.readDataFrameFromBlobAsTSV(configReader, "enrollmentDim.ingest.tables.enrollmentPrograms", "enrollmentPrograms", inputArgsMap,  spark)
    val agreementTypes = DataFrameUtil.readDataFrameFromBlobAsTSV(configReader, "enrollmentDim.ingest.tables.agreementTypes", "agreementTypes", inputArgsMap,  spark)
    val licenseAgreementTypes = DataFrameUtil.readDataFrameFromBlobAsTSV(configReader, "enrollmentDim.ingest.tables.licenseAgreementTypes", "licenseAgreementTypes", inputArgsMap,  spark)
    val countries = DataFrameUtil.readDataFrameFromBlobAsTSV(configReader, "enrollmentDim.ingest.tables.countries", "countries", inputArgsMap,  spark)
    val currencies = DataFrameUtil.readDataFrameFromBlobAsTSV(configReader, "enrollmentDim.ingest.tables.currencies", "currencies", inputArgsMap,  spark)
    val enrollmentContractTypes = DataFrameUtil.readDataFrameFromBlobAsTSV(configReader, "enrollmentDim.ingest.tables.enrollmentContractTypes", "enrollmentContractTypes", inputArgsMap,  spark)

    val enrollmentAccount = DataFrameUtil.readDataFrameFromBlobAsTSV(configReader, "enrollmentDim.ingest.tables.enrollmentAccount", "enrollmentAccount", inputArgsMap,  spark)
    val accountSubscriptionDF = DataFrameUtil.readDataFrameFromBlobAsTSV(configReader, "enrollmentDim.ingest.tables.accountSubscription", "accountSubscription", inputArgsMap,  spark)
    val subscriptionDF = DataFrameUtil.readDataFrameFromBlobAsTSV(configReader, "enrollmentDim.ingest.tables.subscription", "subscription", inputArgsMap,  spark)
    val stageSubscriptionSnapshotDF = DataFrameUtil.readDataFrameFromBlobAsParquet(configReader, "enrollmentDim.ingest.tables.stageSubscription", "stageSubscription", inputArgsMap,  spark)


    val finalDF = createFinalDF(enrollmentDF, statusDF, externalStatusDF, operationsCenters, regions,
      enrollmentPrograms, agreementTypes, enrollmentContractTypes, licenseAgreementTypes, countries, currencies,
      enrollmentAccount, accountSubscriptionDF, subscriptionDF, stageSubscriptionSnapshotDF)

    VNextUtil.processFinalDataFrameNew(configReader, spark, finalDF, "enrollmentDim.files.historicalPath", "enrollmentDim.files.mergedPath", inputArgsMap,  "EnrollmentDim")

  }

  def createFinalDF(enrollmentDF: DataFrame, statusDF: DataFrame, externalStatusDF: DataFrame, operationsCenterDF: DataFrame, regionsDF: DataFrame,
                    enrollmentProgramDF: DataFrame, agreementTypesDF: DataFrame, enrollmentContractTypes: DataFrame, licenseAgreementTypeDF: DataFrame,
                    countriesDF: DataFrame, currenciesDF: DataFrame, enrollmentAccountDF: DataFrame, accountSubscriptionDF: DataFrame,
                    subscriptionDF: DataFrame, stageSubscriptionDF: DataFrame
                   ): DataFrame = {

    val directChannels = Seq("MpsaDirect", "EaDirect")
    val indirectChannels = Seq("MpsaIndirect", "EaIndirect")


    val  tempDF= enrollmentDF.as("e").
      join(statusDF.as("s"), col("s.Id") === col("e.StatusId") , "left").
      join(externalStatusDF.as("als"), col("als.Id") === col("e.AuthLevelStatusId"), "left").
      join(operationsCenterDF.as("oc"), col("oc.Id") === col("e.OperationsCenterId"), "left").
      join(regionsDF.as("r"), col("r.Id") === col("e.RegionId"), "left").
      join(enrollmentProgramDF.as("ep"), col("ep.Id") === col("e.ProgramId"), "left").
      join(agreementTypesDF.as("at"), col("at.Id") === col("e.AgreementTypeId"), "left").
      join(enrollmentContractTypes.as("ect"), col("ect.Id") === col("e.ContractTypeId"), "left").
      join(licenseAgreementTypeDF.as("lat"), col("lat.Id") === col("e.LicenseAgreementTypeId"), "left").
      join(countriesDF.as("c"), col("c.Id") === col("e.CountryId"), "left").
      join(currenciesDF.as("cr"), col("cr.Id") === col("e.PriceListCurrencyId"), "left")
      .select(
        col("e.EndCustomerName"),
        col("e.Id").as("EnrollmentId"),
        col("e.AmendmentType"),
        col("e.StatusId"),
        col("s.name").as("Status"),
        col("s.ExternalStatusCode").as("StatusCode"),
        col("e.StartEffectiveDate"),
        col("e.EndEffectiveDate"),
        col("e.AmendmentStartDate"),
        col("e.AmendmentEndDate"),
        col("e.EnrollmentNumber"),
        col("e.ProgramId"),
        col("ep.Name").as("ProgramName"),
        col("ep.Code").as("ProgramCode"),
        col("e.ProgramOfferingCode"),
        col("e.OfferingLevelCode"),
        col("e.EnrollmentKey"),
        col("e.OperationsCenterId"),
        col("oc.Name").as("OperationsCenterName"),
        col("oc.Code").as("OperationsCenterCode"),
        col("e.PriceListCurrencyId"),
        col("cr.CurrencyName"),
        col("cr.CurrencyCode"),
        col("e.BillingCycle"),
        col("e.GuaranteedServicePercentage"),
        col("e.CountryId"),
        col("c.Name").as("CountryName"),
        col("c.ISOCountryCode"),
        col("e.RegionId"),
        col("r.Name").as("Region"),
        col("r.Code").as("RegionCode"),
        col("e.CreatedOn"),
        col("e.ModifiedOn"),
        col("e.AuthLevelStatusId"),
        col("als.Name").as("AuthLevelStatus"),
        col("als.ExternalStatusCode").as("AuthLevelStatusCode"),
        col("e.InvoiceNotificationCycle"),
        col("e.PublicCustomerNumber"),
        col("e.PriorEnrollmentKey"),
        col("e.TransferTo"),
        col("e.TransferDate"),
        col("e.HermesStartDate"),
        col("e.MPNId"),
        col("e.TerminateSuccessful"),
        col("e.TransferDepartments"),
        col("e.AgreementTypeId"),
        col("at.Name").as("AgreementType"),
        col("at.Code").as("AgreementTypeCode"),
        col("e.ContractTypeId"),
        col("ect.Name").as("ContracttypeName"),
        col("ect.Code").as("ContracttypeCode"),
        col("e.LicenseAgreementTypeId"),
        col("lat.Name").as("LicenseAgreementTypeName"),
        col("lat.Code").as("LicenseAgreementTypeCode"),
        when(col("e.Channel").isin(directChannels: _*), "Direct Enterprise")
          .when(col("e.Channel").isin(indirectChannels: _*), "Indirect Enterprise")
          .otherwise(lit(null).cast(StringType)).as("Channel"),
        col("e.Version"),
        col("e.Attributes"),
        col("e.EnrollmentNumber").as("AgreementNumber"),
        col("als.RelatedType").as("ALSRelatedType"),
        col("s.RelatedType").as("RelatedType")
      )
    tempDF.persist()


    val enrollmentStatusJoinedTempDF  =  enrollmentAccountDF.as("ea").
      join(accountSubscriptionDF.as("asa"), col("asa.AccountId") === col("ea.AccountId"), "inner").
      join(subscriptionDF.as("su"), col("su.AccountId") === col("asa.AccountId") && col("su.Id") === col("asa.SubscriptionId"), "inner").
      join(stageSubscriptionDF.as("sd"), lower(col("su.MOCPSubscriptionGuid")) === lower(col("sd.AzureSubscriptionId")), "inner").
      select(col("ea.EnrollmentId")).distinct()


    var enrollmentStatusJoinedDF  =  tempDF.as("t").
      join(enrollmentStatusJoinedTempDF.as("ea"), col("t.EnrollmentId") === col("ea.EnrollmentId"), "inner").

      select(col("EndCustomerName"),
        col("t.EnrollmentId"),
        col("AmendmentType"),
        col("StatusId"),
        col("Status"),
        col("StatusCode"),
        col("StartEffectiveDate"),
        col("EndEffectiveDate"),
        col("AmendmentStartDate"),
        col("AmendmentEndDate"),
        col("EnrollmentNumber"),
        col("ProgramId"),
        col("ProgramName"),
        col("ProgramCode"),
        col("ProgramOfferingCode"),
        col("OfferingLevelCode"),
        col("EnrollmentKey"),
        col("OperationsCenterId"),
        col("OperationsCenterName"),
        col("OperationsCenterCode"),
        col("PriceListCurrencyId"),
        col("CurrencyName"),
        col("CurrencyCode"),
        col("BillingCycle"),
        col("GuaranteedServicePercentage"),
        col("CountryId"),
        col("CountryName"),
        col("ISOCountryCode"),
        col("RegionId"),
        col("Region"),
        col("RegionCode"),
        col("CreatedOn"),
        col("ModifiedOn"),
        col("AuthLevelStatusId"),
        col("AuthLevelStatus"),
        col("AuthLevelStatusCode"),
        col("InvoiceNotificationCycle"),
        col("PublicCustomerNumber"),
        col("PriorEnrollmentKey"),
        col("TransferTo"),
        col("TransferDate"),
        col("HermesStartDate"),
        col("MPNId"),
        col("TerminateSuccessful"),
        col("TransferDepartments"),
        col("AgreementTypeId"),
        col("AgreementType"),
        col("AgreementTypeCode"),
        col("ContractTypeId"),
        col("ContracttypeName"),
        col("ContracttypeCode"),
        col("LicenseAgreementTypeId"),
        col("LicenseAgreementTypeName"),
        col("LicenseAgreementTypeCode"),
        col("Channel"),
        col("Version"),
        col("Attributes"),
        col("AgreementNumber"),
        col("ALSRelatedType"),
        col("RelatedType"))
      .where(lower(col("t.ALSRelatedType")) === "authlevel" && lower(col("t.RelatedType")) === "enrollment")

    println("The rows finally are " + enrollmentStatusJoinedDF.count())

    enrollmentStatusJoinedDF = DataFrameUtil.fillDefaultValuesForNullColumns(enrollmentStatusJoinedDF)
    //enrollmentStatusJoinedDF = enrollmentStatusJoinedDF.withColumn("AuthLevelStatusCode",lit(""))
    //enrollmentStatusJoinedDF = enrollmentStatusJoinedDF.withColumn("TransferDepartments",lit(""))
    val finalDFWithTimeDF = DataFrameUtil.convertDateColumnsToString(enrollmentStatusJoinedDF, Seq("StartEffectiveDate","EndEffectiveDate", "AmendmentStartDate", "AmendmentEndDate",
      "TransferDate", "HermesStartDate", "CreatedOn", "ModifiedOn"))
    val finalDF = finalDFWithTimeDF.withColumn("rowKey", col("EnrollmentId"))
    finalDF

  }

}
