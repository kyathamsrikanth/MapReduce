package com.srikanth.cs441
package TaskTwo

import CommonUtil.GetConfigRef

import com.srikanth.cs441.CommonUtil.CommonMethods.{checkRegexPattern, checkTimeInterval}
import com.srikanth.cs441.CommonUtil.GetConfigRef.{getDesignatedRegexPattern, getLogTimeFormatRegex}
import org.apache.hadoop.fs.Path
import org.apache.hadoop.io.{IntWritable, LongWritable, Text}
import org.apache.hadoop.mapred.{FileInputFormat, FileOutputFormat, JobClient, JobConf, MapReduceBase, Mapper, OutputCollector, Reducer, Reporter, TextInputFormat, TextOutputFormat}
import org.slf4j.LoggerFactory

import scala.collection.JavaConverters.*
import java.io.IOException
import java.text.SimpleDateFormat
import java.time.Duration
import java.util
import java.util.Date
/**
 * Task 2 :  You will compute time intervals sorted in the descending order that contained most log messages
   of the type ERROR with injected regex pattern string instances .

   Intuition : To Sort the Output By values one can see that if we pass the value(count) as the key to the second reducer ,
   Default Combiner Class will group By and sort based on key(count) before sending ton reducer So our output will be in sorted order.

 * Mapper One  Algorithm :
 *  1. Check for the time regex pattern in the line ignore the line if it does not contain time stamp .
 *  2. Check if the time stamp in in between pre defined interval mentioned in application conf .
 *  3. Check if line contains LOG type ERROR  defined in application conf .
 *  4. Check if the line designated regex pattern .
 *  5  If all above conditions met the we have valid line .
 *  6. We will find The bucket interval of that timestamp which will be key and value will the one as our mapper output .
 *
 *  Reducer One  Algorithm :
 *  1. Sum up the values for each bucket interval
 *  2. Output ==> (bucket interval,Sum)
 */
object TaskTwoMapReduce:
  class TaskTwoMapperOne extends MapReduceBase with Mapper[LongWritable, Text, Text, IntWritable] :
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
      val TimeIntervalLength = getTimeIntervalLength.toInt
      logger.info("Validating the Log line")
      //  Check for the time regex pattern in the line ignore the line if it does not contain time stamp
      if (checkRegexPattern(line, logTimeFormatRegex)) {
        val intervalStartTime = new SimpleDateFormat("HH:mm").parse(preDefinedTimeInterval._1)
        val intervalEndTime = new SimpleDateFormat("HH:mm").parse(preDefinedTimeInterval._2)
        val logTimeStamp = new SimpleDateFormat("HH:mm").parse(logTimeFormatRegex.findFirstIn(line).get)
        // Check if the time stamp in in between pre defined interval mentioned in application conf
        // Check if line contains LOG type defined in application conf
        //Check if the line designated regex pattern
        if (checkTimeInterval(intervalStartTime,intervalEndTime, logTimeStamp)
          && checkRegexPattern(line, logMessageTypes)
          && checkRegexPattern(line, designatedRegexPattern)) {
          logger.info("Fetching time Bucket of the Time Stamp : " + logTimeStamp)
          // get the Time Bucket for the given Time Stamp
          val timeBucketInterval = getTimeBucketInterval(intervalStartTime,intervalEndTime,logTimeStamp,TimeIntervalLength)
          output.collect(new Text(timeBucketInterval._1 + "-" +timeBucketInterval._2), one)
        }
      }
    end map

    private def getPreDefinedTimeInterval: Tuple2[String, String] =
      val intervalTimeFrame = config.getStringList(s"mapReduceTaskTwo.PreDefinedTimeInterval").asScala.toList
      // Check if it is Valid Time Interval given in the Config
      if intervalTimeFrame.length != 2 then throw new IllegalArgumentException(s"Incorrect range of values is specified for PreDefinedTimeInterval")
      (intervalTimeFrame(0), intervalTimeFrame(1))
    end getPreDefinedTimeInterval


    def getTimeBucketInterval(intervalStartTime: Date,intervalEndTime: Date, logTimeStamp: Date,TimeIntervalLength : Integer): Tuple2[String, String] =
      // calculate The Time difference between start time and log time stamp in minutes
      val timeDiff  = (logTimeStamp.getTime - intervalStartTime.getTime)/(60 * 1000)
      // find the bucket in which the  given time stamp will lie
      val bucketNumber =  timeDiff/TimeIntervalLength
      // find the start time of the bucket based oin start time and bucket number 
      val startTimeBucketInterval = Date.from(intervalStartTime.toInstant.plus(Duration.ofMinutes(bucketNumber * TimeIntervalLength)))
      // find the end time of the bucket based oin start time and bucket number 
      val endTimeBucketInterval = intervalStartTime.toInstant.plus(Duration.ofMinutes((bucketNumber+1)*TimeIntervalLength))
      val minEndTimeInterval = if(endTimeBucketInterval.compareTo(intervalEndTime.toInstant) > 0) intervalEndTime else  Date.from(endTimeBucketInterval)
      (startTimeBucketInterval.getHours.toString + ":" + startTimeBucketInterval.getMinutes.toString,minEndTimeInterval.getHours.toString +":" + minEndTimeInterval.getMinutes.toString)
    end getTimeBucketInterval

    private def getLogMessageTypes: String =
      logger.info("Fetching mapReduceTaskTwo LogMessageTypes ")
      val intervalTimeFrame = config.getString(s"mapReduceTaskTwo.LogMessageTypes")
      intervalTimeFrame
    end getLogMessageTypes

    private def getTimeIntervalLength: String =
      logger.info("Fetching mapReduceTaskTwo TimeIntervalLength ")
      val intervalTimeFrame = config.getString(s"mapReduceTaskTwo.TimeIntervalLength")
      intervalTimeFrame
    end getTimeIntervalLength


  class TaskTwoReducerOne extends MapReduceBase with Reducer[Text, IntWritable, Text, IntWritable] :
    override def reduce(key: Text, values: util.Iterator[IntWritable], output: OutputCollector[Text, IntWritable], reporter: Reporter): Unit =
      //  Sum up the values for each Time frame
      val sum = values.asScala.reduce((valueOne, valueTwo) => new IntWritable(valueOne.get() + valueTwo.get()))
      output.collect(key, new IntWritable(sum.get()))

  /**
   *
   * Mapper Two  Algorithm :
   *  1. In order to sort in descending order we can simple take negative counts so that when combiner sorts out will be in descending order .
   *  2. So Output of the Mapper will be (negativeCount, timeFrame) .

   *
   * Reducer Two  Algorithm :
   *  1. Covert the Count value back to positive and output each value .
   */
  class TaskTwoMapperTwo extends MapReduceBase with Mapper[LongWritable, Text, LongWritable, Text] :
    @throws[IOException]
    override def map(key: LongWritable, value: Text, output: OutputCollector[LongWritable, Text], reporter: Reporter): Unit =
      val mapperOutput = value.toString.split(',')
      val timeFrame = new Text(mapperOutput(0))
      // multiplying with -1 to sort in descending
      val negativeCount = new LongWritable((mapperOutput(1).toInt) * -1)
      output.collect(negativeCount, timeFrame)
    end map


  class TaskTwoReducerTwo extends MapReduceBase with Reducer[LongWritable, Text, Text, LongWritable] :
    override def reduce(key: LongWritable, values: util.Iterator[Text], output: OutputCollector[Text, LongWritable], reporter: Reporter): Unit =
      // revert back to positive by multiplying -1
      val positiveCount = new LongWritable(key.get() * -1)
      values.asScala.foreach(value => {output.collect(value, positiveCount)})
