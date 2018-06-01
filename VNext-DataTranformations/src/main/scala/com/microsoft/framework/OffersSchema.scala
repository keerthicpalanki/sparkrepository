package com.microsoft.framework

import org.apache.spark.sql.types._

object OffersSchema {

  def getOffersSchema() : StructType = {
    val offerSchema = StructType(Seq(
      StructField("offers", ArrayType(StructType(Array(
        StructField("Version", StringType, true),
        StructField("OfferId", StringType, true),
        StructField("VirtualMachineImagesByServicePlan", StringType, true),
        StructField("Languages", StringType, true),
        StructField("PublisherId", StringType, true),
        StructField("PublisherName", StringType, true),
        StructField("PublisherLegalName", StringType, true),
        StructField("PublisherWebsite", StringType, true),
        StructField("Store", StringType, true),
        StructField("Timestamp", StringType, true),
        StructField("PublisherNaturalIdentifier", StringType, true),
        StructField("ServiceNaturalIdentifier", StringType, true),
        StructField("SupportUri", StringType, true),
        StructField("IntegrationContactName", StringType, true),
        StructField("IntegrationContactEmail", StringType, true),
        StructField("IntegrationContactPhoneNumber", StringType, true),
        StructField("Categories", StringType, true),
        StructField("DataServiceId", StringType, true),
        StructField("DataServiceVersion", StringType, true),
        StructField("DataServiceListingUrl", StringType, true),
        StructField("ResourceType", StringType, true),
        StructField("ResourceProviderIdentifier", StringType, true),
        StructField("ResourceManifest", StringType, true),
        StructField("FulfillmentServiceId", StringType, true),
        StructField("AvailableDataCenters", StringType, true),
        StructField("ServicePlansByMarket", StringType, true)

//      StructField("IsPreview", StringType, true),
//      StructField("OfferType", StringType, true),
//      StructField("OfferName", StringType, true),
//      StructField("PublisherNID", StringType, true)
      ))))
    ))
    offerSchema
  }

}
