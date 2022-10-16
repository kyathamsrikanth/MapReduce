package com.srikanth.cs441
package CommonUtil

import org.scalatest.funsuite.AnyFunSuite

import java.text.SimpleDateFormat

class CommonMethodsTest extends AnyFunSuite {

  test("testCheckRegexPattern") {
    val line = "19:36:31.037 [scala-execution-context-global-14] INFO  HelperUtils.Parameters$ - 2Xzz1{p?[E5waf2bf1cg3be29UWFDZMIY"
    val Output = CommonMethods.checkRegexPattern(line,"INFO".r)
    assert(Output)
  }

  test("testCheckTimeInterval") {
    val intervalStartTime = new SimpleDateFormat("HH:mm").parse("12:23")
    val intervalEndTime = new SimpleDateFormat("HH:mm").parse("12:45")
    val time = new SimpleDateFormat("HH:mm").parse("12:37")
    val output = CommonMethods.checkTimeInterval(intervalStartTime,intervalEndTime,time)

    assert(output)
  }

}
