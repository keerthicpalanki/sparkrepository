package com.microsoft.common

import com.typesafe.config.Config

class ColumnSchema(config : Config ) {

  val ordinal = config.getInt("ordinal")
  val name = config.getString("name")
  val dataType = config.getString("dataType")
  val sourceFormat = if (config.hasPath("sourceFormat")) config.getString("sourceFormat") else null
  val extractPattern : String = if (config.hasPath("extractPattern")) config.getString("extractPattern") else null
  val nullable : Boolean = if (config.hasPath("nullable")) {
    val value = config.getString("nullable")
    value.toLowerCase() match {
      case "false" => false
      case _ => true
    }} else true
}