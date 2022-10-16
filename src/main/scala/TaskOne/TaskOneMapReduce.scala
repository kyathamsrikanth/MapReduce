package com.srikanth.cs441
package TaskOne


import com.srikanth.cs441.CommonUtil.CommonMethods.{checkRegexPattern, checkTimeInterval}
import com.srikanth.cs441.CommonUtil.GetConfigRef
import com.srikanth.cs441.CommonUtil.GetConfigRef.*
import org.apache.hadoop.conf.*
import org.apache.hadoop.fs.Path
import org.apache.hadoop.io.*
import org.apache.hadoop.util.*
import org.apache.hadoop.mapred.*
import org.slf4j.LoggerFactory

import scala.collection.JavaConverters.*
import java.io.IOException
import java.text.SimpleDateFormat
import java.util
import java.util.Date
import scala.util.matching.Regex
/**
 * This class is made to execute the first part of the functionality which shows the distribution of different type of messages across predefined time intervals (application.conf).
 * First mapreduce job (Filter + count):
 *  The logic used is to first parse each line and extract the intervals in the matcher. Now, the time is parsed and the log level is parsed.
 *  The string is then matched with the regex pattern defined in the application.conf file and if it matches, the mapper is instructed to write to context the corresponding time interval, log level and 'one' which is an intWritable.
 *  Here, the interval number is a value obtained by an algorithm which produces appropriate groups based on the time interval. This logic can be seen in line 67.
 *  The reducer sums the matched values for each group (time interval, log level).
 * Second mapreduce job (Time_interval correction job):
 *  The logic used is to split each line to extract the interval number and convert it back into mm:ss format to be put in the output. This splitting logic is implemented by the mapper whereas the reducer does not perform any special operation.
 *
 * The final output is in the following format:
 *    Interval start time (mm:ss)  |  Log Level  |  Number of matching strings
 */
object  TaskOneMapReduce {
  class TaskOneMapper extends MapReduceBase with Mapper[LongWritable, Text, Text, IntWritable] :
    private final val one = new IntWritable(1)
    private val logger = LoggerFactory.getLogger(getClass)
    private val config = GetConfigRef("mapReduceTaskOne") match {
      case Some(value) => value
      case None => throw new RuntimeException("Cannot obtain a reference to the config data.")
    }

    @throws[IOException]
    override def map(key: LongWritable, value: Text, output: OutputCollector[Text, IntWritable], reporter: Reporter): Unit =
      val line = value.toString
      val preDefinedTimeInterval = getPreDefinedTimeInterval
      val designatedRegexPattern = getDesignatedRegexPattern.r
      val logMessageTypes = getLogMessageTypes.r
      val logTimeFormatRegex = getLogTimeFormatRegex.r

      if (checkRegexPattern(line, logTimeFormatRegex)) {
        val intervalStartTime = new SimpleDateFormat("HH:mm:ss.SSS").parse(preDefinedTimeInterval(0))
        val intervalEndTime = new SimpleDateFormat("HH:mm:ss.SSS").parse(preDefinedTimeInterval(1))
        val logTimeStamp = new SimpleDateFormat("HH:mm:ss.SSS").parse(logTimeFormatRegex.findFirstIn(line).get)

        if (checkTimeInterval(intervalStartTime, intervalEndTime, logTimeStamp)
          && checkRegexPattern(line, logMessageTypes)
          && checkRegexPattern(line, designatedRegexPattern)) {
          output.collect(new Text(logMessageTypes.findFirstIn(line).get), one)
        }
      }
    end map

    def getPreDefinedTimeInterval: Tuple2[String, String] =
      val intervalTimeFrame = config.getStringList(s"mapReduceTaskOne.PreDefinedTimeInterval").asScala.toList
      if intervalTimeFrame.length != 2 then throw new IllegalArgumentException(s"Incorrect range of values is specified for PreDefinedTimeInterval")
      (intervalTimeFrame(0), intervalTimeFrame(1))
    end getPreDefinedTimeInterval

  class TaskOneReducer extends MapReduceBase with Reducer[Text, IntWritable, Text, IntWritable] :
    override def reduce(key: Text, values: util.Iterator[IntWritable], output: OutputCollector[Text, IntWritable], reporter: Reporter): Unit =
      val sum = values.asScala.reduce((valueOne, valueTwo) => new IntWritable(valueOne.get() + valueTwo.get()))
      output.collect(key, new IntWritable(sum.get()))
  
}
