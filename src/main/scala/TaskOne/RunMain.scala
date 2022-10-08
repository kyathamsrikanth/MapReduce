package com.srikanth.cs441
package TaskOne


import com.srikanth.cs441.TaskOne.TaskOneMapper.TaskOneMapper
import com.srikanth.cs441.TaskOne.TaskOneReducer.TaskOneReducer
import org.apache.hadoop.conf.*
import org.apache.hadoop.fs.Path
import org.apache.hadoop.io.*
import org.apache.hadoop.util.*
import org.apache.hadoop.mapred.*
object  RunMain {

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
