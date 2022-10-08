package com.srikanth.cs441
package TaskOne


import com.srikanth.cs441.CommonUtil.GetConfigRef
import com.srikanth.cs441.CommonUtil.GetConfigRef.{getDesignatedRegexPattern, getLogMessageTypes, getLogTimeFormatRegex}
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

    private def getPreDefinedTimeInterval: Tuple2[String, String] =
      val intervalTimeFrame = config.getStringList(s"mapReduceTaskOne.PreDefinedTimeInterval").asScala.toList
      if intervalTimeFrame.length != 2 then throw new IllegalArgumentException(s"Incorrect range of values is specified for PreDefinedTimeInterval")
      (intervalTimeFrame(0), intervalTimeFrame(1))
    end getPreDefinedTimeInterval

    private def checkTimeInterval(intervalStartTime: Date, intervalEndTime: Date, logTimeStamp: Date): Boolean =
      logTimeStamp.compareTo(intervalStartTime) >= 0 && intervalEndTime.compareTo(logTimeStamp) >= 0
    end checkTimeInterval

    private def checkRegexPattern(line: String, regexPattern: Regex): Boolean =
      regexPattern.findFirstIn(line).isDefined
    end checkRegexPattern

  class TaskOneReducer extends MapReduceBase with Reducer[Text, IntWritable, Text, IntWritable] :
    override def reduce(key: Text, values: util.Iterator[IntWritable], output: OutputCollector[Text, IntWritable], reporter: Reporter): Unit =
      val sum = values.asScala.reduce((valueOne, valueTwo) => new IntWritable(valueOne.get() + valueTwo.get()))
      output.collect(key, new IntWritable(sum.get()))

  @main def runMapReduce(inputPath: String, outputPath: String) =
    val conf: JobConf = new JobConf(this.getClass)
    conf.setJobName("MapReduceTask1")
    //conf.set("fs.defaultFS", "hdfs://localhost:9000")
    conf.set("fs.defaultFS", "local")
    conf.set("mapreduce.job.maps", "1")
    conf.set("mapreduce.job.reduces", "1")
    conf.setOutputKeyClass(classOf[Text])
    conf.setOutputValueClass(classOf[IntWritable])
    conf.setMapperClass(classOf[TaskOneMapper])
    conf.setCombinerClass(classOf[TaskOneReducer])
    conf.setReducerClass(classOf[TaskOneReducer])
    conf.setInputFormat(classOf[TextInputFormat])
    conf.setOutputFormat(classOf[TextOutputFormat[Text, IntWritable]])
    FileInputFormat.setInputPaths(conf, new Path(inputPath))
    FileOutputFormat.setOutputPath(conf, new Path(outputPath))
    JobClient.runJob(conf)
}
