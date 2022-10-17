package com.srikanth.cs441

import TaskOne.TaskOneMapReduce.{TaskOneMapper, TaskOneReducer}

import com.srikanth.cs441.CommonUtil.GetConfigRef
import com.srikanth.cs441.TaskFour.TaskFourMapReduce.{TaskFourMapper, TaskFourReducer}
import com.srikanth.cs441.TaskThree.TaskThreeMapReduce.{TaskThreeMapper, TaskThreeReducer}
import com.srikanth.cs441.TaskTwo.TaskTwoMapReduce.{TaskTwoMapperOne, TaskTwoMapperTwo, TaskTwoReducerOne, TaskTwoReducerTwo}
import org.apache.hadoop.fs.Path
import org.apache.hadoop.io.{IntWritable, Text}
import org.apache.hadoop.mapred.{FileInputFormat, FileOutputFormat, JobClient, JobConf, TextInputFormat, TextOutputFormat}

object RunMain {


  @main def runMapReduce(inputPath: String, outputPath: String,TaskNumber : String) =

    val config = GetConfigRef("mapReduceConfig") match {
      case Some(value) => value
      case None => throw new RuntimeException("Cannot obtain a reference to the config data.")
    }
    if(TaskNumber.contentEquals("1")){

      val conf: JobConf = new JobConf(this.getClass)
      conf.setJobName("MapReduceTask1")
      conf.set("mapreduce.job.maps", config.getString("mapReduceConfig.mapreduce_job_maps"))
      conf.set("mapreduce.job.reduces", config.getString("mapReduceConfig.mapreduce_job_reduces"))
      conf.set("mapred.textoutputformat.separator",  config.getString("mapReduceConfig.mapred_textoutputformat_separator"));
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
    if(TaskNumber.contentEquals("2")) {
        val conf: JobConf = new JobConf(this.getClass)
        conf.setJobName("MapReduceTask2")
        conf.set("mapreduce.job.maps", config.getString("mapReduceConfig.mapreduce_job_maps"))
        conf.set("mapreduce.job.reduces", config.getString("mapReduceConfig.mapreduce_job_reduces"))
        conf.set("mapred.textoutputformat.separator", config.getString("mapReduceConfig.mapred_textoutputformat_separator"));
        conf.setOutputKeyClass(classOf[Text])
        conf.setOutputValueClass(classOf[IntWritable])
        conf.setMapperClass(classOf[TaskTwoMapperOne])
        conf.setCombinerClass(classOf[TaskTwoReducerOne])
        conf.setReducerClass(classOf[TaskTwoReducerOne])
        conf.setInputFormat(classOf[TextInputFormat])
        conf.setOutputFormat(classOf[TextOutputFormat[Text, IntWritable]])
        FileInputFormat.setInputPaths(conf, new Path(inputPath))
        FileOutputFormat.setOutputPath(conf, new Path(outputPath + "_JOB1Output"))
        JobClient.runJob(conf)
        val conf2: JobConf = new JobConf(this.getClass)
        conf2.setJobName("MapReduceTask2")
        conf.set("mapreduce.job.maps", config.getString("mapReduceConfig.mapreduce_job_maps"))
        conf.set("mapreduce.job.reduces", config.getString("mapReduceConfig.mapreduce_job_reduces"))
        conf.set("mapred.textoutputformat.separator", config.getString("mapReduceConfig.mapred_textoutputformat_separator"));
        conf2.setMapperClass(classOf[TaskTwoMapperTwo])
        conf2.setReducerClass(classOf[TaskTwoReducerTwo])
        FileInputFormat.setInputPaths(conf2, new Path(outputPath + "_JOB1Output"))
        FileOutputFormat.setOutputPath(conf2, new Path(outputPath))
        JobClient.runJob(conf2)
    }
    if(TaskNumber.contentEquals("3")) {
      val conf: JobConf = new JobConf(this.getClass)
      conf.setJobName("MapReduceTask4")
      conf.set("mapreduce.job.maps", config.getString("mapReduceConfig.mapreduce_job_maps"))
      conf.set("mapreduce.job.reduces", config.getString("mapReduceConfig.mapreduce_job_reduces"))
      conf.set("mapred.textoutputformat.separator", config.getString("mapReduceConfig.mapred_textoutputformat_separator"));
      conf.setOutputKeyClass(classOf[Text])
      conf.setOutputValueClass(classOf[IntWritable])
      conf.setMapperClass(classOf[TaskThreeMapper])
      conf.setCombinerClass(classOf[TaskThreeReducer])
      conf.setReducerClass(classOf[TaskThreeReducer])
      conf.setInputFormat(classOf[TextInputFormat])
      conf.setOutputFormat(classOf[TextOutputFormat[Text, IntWritable]])
      FileInputFormat.setInputPaths(conf, new Path(inputPath))
      FileOutputFormat.setOutputPath(conf, new Path(outputPath))
      JobClient.runJob(conf)
    }
    if(TaskNumber.contentEquals("4")) {
      
      val conf: JobConf = new JobConf(this.getClass)
      conf.setJobName("MapReduceTask4")
      conf.set("mapreduce.job.maps", config.getString("mapReduceConfig.mapreduce_job_maps"))
      conf.set("mapreduce.job.reduces", config.getString("mapReduceConfig.mapreduce_job_reduces"))
      conf.set("mapred.textoutputformat.separator", config.getString("mapReduceConfig.mapred_textoutputformat_separator"));
      conf.setOutputKeyClass(classOf[Text])
      conf.setOutputValueClass(classOf[IntWritable])
      conf.setMapperClass(classOf[TaskFourMapper])
      conf.setCombinerClass(classOf[TaskFourReducer])
      conf.setReducerClass(classOf[TaskFourReducer])
      conf.setInputFormat(classOf[TextInputFormat])
      conf.setOutputFormat(classOf[TextOutputFormat[Text, IntWritable]])
      FileInputFormat.setInputPaths(conf, new Path(inputPath))
      FileOutputFormat.setOutputPath(conf, new Path(outputPath))
      JobClient.runJob(conf)
    }


}
