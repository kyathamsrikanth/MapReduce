package com.srikanth.cs441
package CommonUtil

import CommonUtil.GetConfigRef.config

import java.util.Date
import scala.util.matching.Regex

object CommonMethods :
  /**
   * checkRegexPattern: Checks Regex Pattern and returns True if Found else False
   *
   * @param : line[String]
   * @param :Regex[regexPattern]
   * @return : Boolean
   */
  def checkRegexPattern(line: String, regexPattern: Regex): Boolean =
    regexPattern.findFirstIn(line).isDefined
  end checkRegexPattern

  /**
   * checkTimeInterval: Checks if a given Time Stamp lies in between given Start and End Time Intervals
   *
   * @param :intervalStartTime[String]
   * @param :intervalEndTime[ regexPattern]
   * @param  :intervalEndTime[ regexPattern]
   * @return : Boolean
   */
  def checkTimeInterval(intervalStartTime: Date, intervalEndTime: Date, logTimeStamp: Date): Boolean =
    logTimeStamp.compareTo(intervalStartTime) >= 0 && intervalEndTime.compareTo(logTimeStamp) >= 0
  end checkTimeInterval


