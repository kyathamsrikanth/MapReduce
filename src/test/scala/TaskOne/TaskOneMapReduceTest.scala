package com.srikanth.cs441
package TaskOne

import com.srikanth.cs441.TaskOne.TaskOneMapReduce.TaskOneMapper
import org.scalatest.funsuite.AnyFunSuite

class TaskOneMapReduceTest extends AnyFunSuite {
  test("testGetLogTimeFormatRegex") {
    val expectedOutput = ("19:35:43.350","19:36:09.114")
    val actualOutput =  new TaskOneMapper().getPreDefinedTimeInterval
    assert(expectedOutput == actualOutput)

  }
}
