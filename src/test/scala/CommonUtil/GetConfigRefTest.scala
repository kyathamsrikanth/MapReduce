package com.srikanth.cs441
package CommonUtil

import org.scalatest.funsuite.AnyFunSuite

class GetConfigRefTest extends AnyFunSuite {

  test("testGetLogTimeFormatRegex") {
    val expectedOutput = "([0-9]{2}:[0-9]{2}:[0-9]{2}.[0-9]{3})"
    val actualOutput = GetConfigRef.getLogTimeFormatRegex
    assert(expectedOutput == actualOutput)

  }

  test("testGetDesignatedRegexPattern") {
    val expectedOutput = "([a-c][e-g][0-3]|[A-Z][5-9][f-w]){5,15}"
    val actualOutput = GetConfigRef.getDesignatedRegexPattern
    assert(expectedOutput == actualOutput)
  }

  test("testGetLogMessageTypes") {
    val expectedOutput = "INFO|DEBUG|WARN|ERROR"
    val actualOutput = GetConfigRef.getLogMessageTypes
    assert(expectedOutput == actualOutput)
  }

}
