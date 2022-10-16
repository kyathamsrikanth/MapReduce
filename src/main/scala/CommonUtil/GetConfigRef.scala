package com.srikanth.cs441
package CommonUtil

import com.typesafe.config.{Config, ConfigFactory}
import org.slf4j.LoggerFactory

import java.util.Date
import scala.util.matching.Regex
import scala.util.{Failure, Success, Try}

object GetConfigRef :
  private val config = ConfigFactory.load()
  private val logger = LoggerFactory.getLogger(classOf[GetConfigRef.type])
  
  def apply(confEntry:String):Option[Config] = Try(config.getConfig(confEntry)) match {
    case Failure(exception) => logger.error(s"Failed to retrieve config entry $confEntry for reason $exception");None
    case Success(_) => Some(config)
  }

  def getDesignatedRegexPattern: String =
    val intervalTimeFrame = config.getString(s"generic.DesignatedRegexPattern")
    intervalTimeFrame
  end getDesignatedRegexPattern

  def getLogMessageTypes: String =
    val intervalTimeFrame = config.getString(s"generic.logMessageTypes")
    intervalTimeFrame
  end getLogMessageTypes

  def getLogTimeFormatRegex: String =
    val intervalTimeFrame = config.getString(s"generic.logTimeFormatRegex")
    intervalTimeFrame
  end getLogTimeFormatRegex