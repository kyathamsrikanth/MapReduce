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
 * Task 1 :  First, you will compute a spreadsheet or an CSV file that shows the distribution of different types of messages
   across predefined time intervals and injected string instances of the designated regex pattern for these log message types

 * Mapper Algorithm :
 *  1. Check for the time regex pattern in the line ignore the line if it does not contain time stamp
 *  2. Check if the time stamp in in between pre defined interval mentioned in application conf
 *  3. Check if line contains LOG type defined in application conf
 *  4. Check if the line designated regex pattern
 *  5 If all above conditions met the we have valid line . So we can output value will be (LogType,1)
 *
 *  Reducer Algorithm :
 *  1. Sum up the values for each LogType
 *  2. Output ==> (LogType,Sum)

 */
object  TaskOneMapReduce {
  class TaskOneMapper extends MapReduceBase with Mapper[LongWritable, Text, Text, IntWritable] :
    private final val one = new IntWritable(1)
    private val logger = LoggerFactory.getLogger(getClass)
    logger.info("Getting The Configrations For Map Reduce Task One")
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
      logger.info("Validating the Log line")
      //  Check for the time regex pattern in the line ignore the line if it does not contain time stamp
      if (checkRegexPattern(line, logTimeFormatRegex)) {
        val intervalStartTime = new SimpleDateFormat("HH:mm:ss.SSS").parse(preDefinedTimeInterval(0))
        val intervalEndTime = new SimpleDateFormat("HH:mm:ss.SSS").parse(preDefinedTimeInterval(1))
        val logTimeStamp = new SimpleDateFormat("HH:mm:ss.SSS").parse(logTimeFormatRegex.findFirstIn(line).get)
      // Check if the time stamp in in between pre defined interval mentioned in application conf  
      // Check if line contains LOG type defined in application conf
      //Check if the line designated regex pattern
        if (checkTimeInterval(intervalStartTime, intervalEndTime, logTimeStamp)
          && checkRegexPattern(line, logMessageTypes)
          && checkRegexPattern(line, designatedRegexPattern)) {
          output.collect(new Text(logMessageTypes.findFirstIn(line).get), one)
        }
      }
    end map

    def getPreDefinedTimeInterval: Tuple2[String, String] =
      val intervalTimeFrame = config.getStringList(s"mapReduceTaskOne.PreDefinedTimeInterval").asScala.toList
      // Check if it is Valid Time Interval given in the Config
      if intervalTimeFrame.length != 2 then throw new IllegalArgumentException(s"Incorrect range of values is specified for PreDefinedTimeInterval")
      (intervalTimeFrame(0), intervalTimeFrame(1))
    end getPreDefinedTimeInterval

  class TaskOneReducer extends MapReduceBase with Reducer[Text, IntWritable, Text, IntWritable] :
    override def reduce(key: Text, values: util.Iterator[IntWritable], output: OutputCollector[Text, IntWritable], reporter: Reporter): Unit =
      // Sum up the values for each LogType
      val sum = values.asScala.reduce((valueOne, valueTwo) => new IntWritable(valueOne.get() + valueTwo.get()))
      output.collect(key, new IntWritable(sum.get()))
  
}
