package com.srikanth.cs441
package TaskFour

import CommonUtil.GetConfigRef.{ getLogMessageTypes}

import com.srikanth.cs441.CommonUtil.CommonMethods.checkRegexPattern

import scala.collection.JavaConverters.*
import org.apache.hadoop.fs.Path
import org.apache.hadoop.io.{IntWritable, LongWritable, Text}
import org.apache.hadoop.mapred.{FileInputFormat, FileOutputFormat, JobClient, JobConf, MapReduceBase, Mapper, OutputCollector, Reducer, Reporter, TextInputFormat, TextOutputFormat}
import org.slf4j.LoggerFactory

import java.io.IOException
import java.util

object TaskFourMapReduce :
  /**
   * Task 4 :  You will produce the number of characters in each log message for each log message type that
   *  contain the highest number of characters in the detected instances of the designated regex pattern
   *
   * Mapper Algorithm :
   *  1. Check if line contains LOG type defined in application conf
   *  2. find the message in the log  line and count the no of characters which is the length of the string
   *  3.  So Mapper Output => (LogType,CharactersCount)
   *
   * Reducer Algorithm :
   *  1. Take Maximum of all the values for given logType
   *  2. Output ==> (LogType,max)
   *
   */
  class TaskFourMapper extends MapReduceBase with Mapper[LongWritable, Text, Text, IntWritable] :
    private val logger = LoggerFactory.getLogger(getClass)

    @throws[IOException]
    override def map(key: LongWritable, value: Text, output: OutputCollector[Text, IntWritable], reporter: Reporter): Unit =
      val logMessageTypes = getLogMessageTypes.r
      val messageList: Array[String] = value.toString.split(" - ").map(str => str.trim())
      if (checkRegexPattern(value.toString, logMessageTypes)) {
        output.collect(new Text(logMessageTypes.findFirstIn(value.toString).get),IntWritable(messageList(messageList.length-1).toCharArray.length))
      }

  class TaskFourReducer extends MapReduceBase with Reducer[Text, IntWritable, Text, IntWritable] :
    override def reduce(key: Text, values: util.Iterator[IntWritable], output: OutputCollector[Text, IntWritable], reporter: Reporter): Unit =
      // Finding Maximum in all values
      val maxCharactersCount = values.asScala.foldLeft(0){(maximum, value) => math.max(maximum, value.get)}
      output.collect(key, new IntWritable(maxCharactersCount))