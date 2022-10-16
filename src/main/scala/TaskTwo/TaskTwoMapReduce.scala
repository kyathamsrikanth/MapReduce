package com.srikanth.cs441
package TaskTwo

import CommonUtil.GetConfigRef

import com.srikanth.cs441.CommonUtil.GetConfigRef.{checkRegexPattern, checkTimeInterval, getDesignatedRegexPattern, getLogMessageTypes, getLogTimeFormatRegex}
import com.srikanth.cs441.TaskOne.TaskOneMapReduce.{TaskOneMapper, TaskOneReducer}
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

      if (checkRegexPattern(line, logTimeFormatRegex)) {
        val intervalStartTime = new SimpleDateFormat("HH:mm").parse(preDefinedTimeInterval._1)
        val intervalEndTime = new SimpleDateFormat("HH:mm").parse(preDefinedTimeInterval._2)
        val logTimeStamp = new SimpleDateFormat("HH:mm").parse(logTimeFormatRegex.findFirstIn(line).get)

        if (checkTimeInterval(intervalStartTime,intervalEndTime, logTimeStamp)
          && checkRegexPattern(line, logMessageTypes)
          && checkRegexPattern(line, designatedRegexPattern)) {

          val timeBucketInterval = getTimeBucketInterval(intervalStartTime,intervalEndTime,logTimeStamp,TimeIntervalLength)
          output.collect(new Text(timeBucketInterval._1 + "-" +timeBucketInterval._2), one)
        }
      }
    end map

    private def getPreDefinedTimeInterval: Tuple2[String, String] =
      val intervalTimeFrame = config.getStringList(s"mapReduceTaskTwo.PreDefinedTimeInterval").asScala.toList
      if intervalTimeFrame.length != 2 then throw new IllegalArgumentException(s"Incorrect range of values is specified for PreDefinedTimeInterval")
      (intervalTimeFrame(0), intervalTimeFrame(1))
    end getPreDefinedTimeInterval


    private def getTimeBucketInterval(intervalStartTime: Date,intervalEndTime: Date, logTimeStamp: Date,TimeIntervalLength : Integer): Tuple2[String, String] =
      val timeDiff  = (logTimeStamp.getTime - intervalStartTime.getTime)/(60 * 1000)
      val bucketNumber =  timeDiff/TimeIntervalLength
      val startTimeBucketInterval = Date.from(intervalStartTime.toInstant.plus(Duration.ofMinutes(bucketNumber * TimeIntervalLength)))
      val endTimeBucketInterval = intervalStartTime.toInstant.plus(Duration.ofMinutes((bucketNumber+1)*TimeIntervalLength))
      val minEndTimeInterval = if(endTimeBucketInterval.compareTo(intervalEndTime.toInstant) > 0) intervalEndTime else  Date.from(endTimeBucketInterval)
      (startTimeBucketInterval.getHours.toString + ":" + startTimeBucketInterval.getMinutes.toString,minEndTimeInterval.getHours.toString +":" + minEndTimeInterval.getMinutes.toString)
    end getTimeBucketInterval

    private def getLogMessageTypes: String =
      val intervalTimeFrame = config.getString(s"mapReduceTaskTwo.LogMessageTypes")
      intervalTimeFrame
    end getLogMessageTypes

    private def getTimeIntervalLength: String =
      val intervalTimeFrame = config.getString(s"mapReduceTaskTwo.TimeIntervalLength")
      intervalTimeFrame
    end getTimeIntervalLength


  class TaskTwoReducerOne extends MapReduceBase with Reducer[Text, IntWritable, Text, IntWritable] :
    override def reduce(key: Text, values: util.Iterator[IntWritable], output: OutputCollector[Text, IntWritable], reporter: Reporter): Unit =
      val sum = values.asScala.reduce((valueOne, valueTwo) => new IntWritable(valueOne.get() + valueTwo.get()))
      output.collect(key, new IntWritable(sum.get()))


  class TaskTwoMapperTwo extends MapReduceBase with Mapper[LongWritable, Text, LongWritable, Text] :
    //private final val one = new IntWritable(1)



    @throws[IOException]
    override def map(key: LongWritable, value: Text, output: OutputCollector[LongWritable, Text], reporter: Reporter): Unit =
      val mapperOutput = value.toString.split(',')
      val timeFrame = new Text(mapperOutput(0))
      val negativeCount = new LongWritable((mapperOutput(1).toInt) * -1)
      output.collect(negativeCount, timeFrame)
    end map


  class TaskTwoReducerTwo extends MapReduceBase with Reducer[LongWritable, Text, Text, LongWritable] :
    override def reduce(key: LongWritable, values: util.Iterator[Text], output: OutputCollector[Text, LongWritable], reporter: Reporter): Unit =
      val positiveCount = new LongWritable(key.get() * -1)
      values.asScala.foreach(value => {output.collect(value, positiveCount)})

//
//  @main def runMapReduce(inputPath: String, outputPath: String) =
//    val conf: JobConf = new JobConf(this.getClass)
//    conf.setJobName("MapReduceTask2")
//    //conf.set("fs.defaultFS", "hdfs://localhost:9000")
//    //conf.set("fs.defaultFS", "local")
//    conf.set("mapreduce.job.maps", "1")
//    conf.set("mapreduce.job.reduces", "1")
//    conf.set("mapred.textoutputformat.separator", ",");
//    conf.setOutputKeyClass(classOf[Text])
//    conf.setOutputValueClass(classOf[IntWritable])
//    conf.setMapperClass(classOf[TaskTwoMapperOne])
//    conf.setCombinerClass(classOf[TaskTwoReducerOne])
//    conf.setReducerClass(classOf[TaskTwoReducerOne])
//    conf.setInputFormat(classOf[TextInputFormat])
//    conf.setOutputFormat(classOf[TextOutputFormat[Text, IntWritable]])
//    FileInputFormat.setInputPaths(conf, new Path(inputPath))
//    FileOutputFormat.setOutputPath(conf, new Path(outputPath+"_JOB1Output"))
//    JobClient.runJob(conf)
//    val conf2: JobConf = new JobConf(this.getClass)
//    conf2.setJobName("MapReduceTask2")
//    //conf2.set("fs.defaultFS", "hdfs://localhost:9000")
//    //conf2.set("fs.defaultFS", "local")
//    conf2.set("mapreduce.job.maps", "1")
//    conf2.set("mapreduce.job.reduces", "1")
//    conf2.set("mapred.textoutputformat.separator", ",");
//    conf2.setMapperClass(classOf[TaskTwoMapperTwo])
//    conf2.setReducerClass(classOf[TaskTwoReducerTwo])
//    FileInputFormat.setInputPaths(conf2, new Path(outputPath+"_JOB1Output"))
//    FileOutputFormat.setOutputPath(conf2, new Path(outputPath))
//    JobClient.runJob(conf2)