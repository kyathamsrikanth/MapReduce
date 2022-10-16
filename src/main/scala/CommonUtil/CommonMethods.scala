package com.srikanth.cs441
package CommonUtil

import CommonUtil.GetConfigRef.config

import java.util.Date
import scala.util.matching.Regex

object CommonMethods :
  /**
   * Reduce Implementation: Aggregates mapper results and generates total number of ERROR, WARN, DEBUG and INFO logs.
   *
   * @param : line[String]
   * @param values
   * @param context
   */
  def checkRegexPattern(line: String, regexPattern: Regex): Boolean =
    regexPattern.findFirstIn(line).isDefined
  end checkRegexPattern
  
  def checkTimeInterval(intervalStartTime: Date, intervalEndTime: Date, logTimeStamp: Date): Boolean =
    logTimeStamp.compareTo(intervalStartTime) >= 0 && intervalEndTime.compareTo(logTimeStamp) >= 0
  end checkTimeInterval


