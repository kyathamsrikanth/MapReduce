package com.srikanth.cs441
package TaskThree

import com.srikanth.cs441.CommonUtil.CommonMethods.checkRegexPattern
import com.srikanth.cs441.CommonUtil.GetConfigRef.{ getLogMessageTypes}
import com.srikanth.cs441.TaskOne.TaskOneMapReduce.{TaskOneMapper, TaskOneReducer}
import org.apache.hadoop.fs.Path
import org.apache.hadoop.io.{IntWritable, LongWritable, Text}
import org.apache.hadoop.mapred.{FileInputFormat, FileOutputFormat, JobClient, JobConf, MapReduceBase, Mapper, OutputCollector, Reducer, Reporter, TextInputFormat, TextOutputFormat}

import scala.collection.JavaConverters.*
import java.util
import org.slf4j.LoggerFactory

import java.io.IOException
object TaskThreeMapReduce :
  /**
   * Task 3 :  For each message type you will produce the number of the generated log messages
   * 
   * Mapper Algorithm :
   *  1. Check if line contains LOG type defined in application conf
   *  2. If all above conditions met the we have valid line . So we can output value will be (LogType,1)
   *
   * Reducer Algorithm :
   *  1. Sum up the values for each LogType
   *  2. Output ==> (LogType,Sum)
   *
   */
  class TaskThreeMapper extends MapReduceBase with Mapper[LongWritable, Text, Text, IntWritable] :
    private final val one = new IntWritable(1)
    private val logger = LoggerFactory.getLogger(getClass)

    @throws[IOException]
    override def map(key: LongWritable, value: Text, output: OutputCollector[Text, IntWritable], reporter: Reporter): Unit =
      val logMessageTypes = getLogMessageTypes.r
      val line = value.toString
      logger.info("Checking for Valid Log Type Pattern in the Log Line")
      // Check if we have valid Log Type in the Line
      if (checkRegexPattern(line, logMessageTypes)) {
        output.collect(new Text(logMessageTypes.findFirstIn(line).get), one)
      }

  class TaskThreeReducer extends MapReduceBase with Reducer[Text, IntWritable, Text, IntWritable] :
    override def reduce(key: Text, values: util.Iterator[IntWritable], output: OutputCollector[Text, IntWritable], reporter: Reporter): Unit =
      // Sum up the values for each LogType
      val sum = values.asScala.reduce((valueOne, valueTwo) => new IntWritable(valueOne.get() + valueTwo.get()))
      output.collect(key, new IntWritable(sum.get()))
