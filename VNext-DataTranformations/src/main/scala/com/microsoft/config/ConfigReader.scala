package com.microsoft.config

import com.typesafe.config.{Config, ConfigFactory}

class ConfigReader(fileName : String){

  var config : Config = null

  def loadConfig()={
      config = ConfigFactory.load(fileName)
  }

  def getValueForKey( key: String) : AnyRef = {
    config match {
      case null => loadConfig
      case _ => None
    }
    config.getAnyRef(key)
  }

  def getConfig() : Config ={
    config match {
      case null => loadConfig
      case _ => None
    }
    config
  }

}