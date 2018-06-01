package com.microsoft.config

import com.typesafe.config.{Config, ConfigFactory}
import org.scalatest.junit.JUnitRunner
import org.junit.runner.RunWith
import org.scalatest.{BeforeAndAfter, FunSuite}

@RunWith(classOf[JUnitRunner])
class ConfigTest extends FunSuite with BeforeAndAfter {

  var config: Config  = null

  before {
    config = ConfigFactory.load("test-application.conf")
  }

  test("test config") {
    assert( true == (config != null))
  }

}