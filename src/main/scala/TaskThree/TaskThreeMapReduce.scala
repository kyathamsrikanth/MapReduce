package com.srikanth.cs441
package TaskThree

import com.srikanth.cs441.CommonUtil.GetConfigRef
import com.srikanth.cs441.CommonUtil.GetConfigRef.{checkRegexPattern, getLogMessageTypes}
import com.srikanth.cs441.TaskOne.TaskOneMapReduce.{TaskOneMapper, TaskOneReducer}
import org.apache.hadoop.fs.Path
import org.apache.hadoop.io.{IntWritable, LongWritable, Text}
import org.apache.hadoop.mapred.{FileInputFormat, FileOutputFormat, JobClient, JobConf, MapReduceBase, Mapper, OutputCollector, Reducer, Reporter, TextInputFormat, TextOutputFormat}

import scala.collection.JavaConverters.*
import java.util
import org.slf4j.LoggerFactory

import java.io.IOException
object TaskThreeMapReduce :
  class TaskThreeMapper extends MapReduceBase with Mapper[LongWritable, Text, Text, IntWritable] :
    private final val one = new IntWritable(1)
    private val logger = LoggerFactory.getLogger(getClass)

    @throws[IOException]
    override def map(key: LongWritable, value: Text, output: OutputCollector[Text, IntWritable], reporter: Reporter): Unit =
      val logMessageTypes = getLogMessageTypes.r
      val line = value.toString

      if (checkRegexPattern(line, logMessageTypes)) {
        output.collect(new Text(logMessageTypes.findFirstIn(line).get), one)
      }

  class TaskThreeReducer extends MapReduceBase with Reducer[Text, IntWritable, Text, IntWritable] :
    override def reduce(key: Text, values: util.Iterator[IntWritable], output: OutputCollector[Text, IntWritable], reporter: Reporter): Unit =
      val sum = values.asScala.reduce((valueOne, valueTwo) => new IntWritable(valueOne.get() + valueTwo.get()))
      output.collect(key, new IntWritable(sum.get()))

//
//  @main def runMapReduce(inputPath: String, outputPath: String) =
//    val conf: JobConf = new JobConf(this.getClass)
//    conf.setJobName("MapReduceTask4")
//    //conf.set("fs.defaultFS", "hdfs://localhost:9000")
//    //conf.set("fs.defaultFS", "local")
//    conf.set("mapreduce.job.maps", "1")
//    conf.set("mapreduce.job.reduces", "1")
//    conf.set("mapred.textoutputformat.separator", ",");
//    conf.setOutputKeyClass(classOf[Text])
//    conf.setOutputValueClass(classOf[IntWritable])
//    conf.setMapperClass(classOf[TaskThreeMapper])
//    conf.setCombinerClass(classOf[TaskThreeReducer])
//    conf.setReducerClass(classOf[TaskThreeReducer])
//    conf.setInputFormat(classOf[TextInputFormat])
//    conf.setOutputFormat(classOf[TextOutputFormat[Text, IntWritable]])
//    FileInputFormat.setInputPaths(conf, new Path(inputPath))
//    FileOutputFormat.setOutputPath(conf, new Path(outputPath))
//    JobClient.runJob(conf)



