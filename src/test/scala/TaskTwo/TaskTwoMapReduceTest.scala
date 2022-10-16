package com.srikanth.cs441
package TaskTwo

import com.srikanth.cs441.TaskTwo.TaskTwoMapReduce.TaskTwoMapperOne
import org.scalatest.funsuite.AnyFunSuite

import java.text.SimpleDateFormat

class TaskTwoMapReduceTest extends AnyFunSuite {

  test("testGetTimeBucketInterval") {
    val expectedOutput = ("12:35", "12:38")
    val intervalStartTime = new SimpleDateFormat("HH:mm").parse("12:23")
    val intervalEndTime = new SimpleDateFormat("HH:mm").parse("12:45")
    val time = new SimpleDateFormat("HH:mm").parse("12:37")
    val actualOutput = new TaskTwoMapperOne().getTimeBucketInterval(intervalStartTime, intervalEndTime, time, 3)
    assert(expectedOutput == actualOutput)

  }

}
