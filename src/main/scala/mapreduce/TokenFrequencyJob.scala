package mapreduce

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.hadoop.mapreduce.Job
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat
import org.apache.hadoop.io.{IntWritable, Text}
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat

// TokenFrequencyJob.scala
class TokenFrequencyJob {

  def runJob(inputPath: String, outputPath: String): Boolean = {
    val conf = new Configuration()
    val fs = FileSystem.get(conf)

    // Delete output path if it exists
    val outputDir = new Path(outputPath)
    if (fs.exists(outputDir)) {
      println(s"Output path $outputPath exists. Deleting it.")
      fs.delete(outputDir, true)
    }

    val job = Job.getInstance(conf, "Token Frequency Job")
    job.setJarByClass(classOf[TokenFrequencyJob])

    // Set Mapper and Reducer classes
    job.setMapperClass(classOf[TokenFrequencyMapper])
    job.setReducerClass(classOf[TokenFrequencyReducer])

    // Set the output key and value types
    job.setOutputKeyClass(classOf[Text])
    job.setOutputValueClass(classOf[IntWritable])

    // Set input and output formats
    job.setInputFormatClass(classOf[TextInputFormat])
    job.setOutputFormatClass(classOf[TextOutputFormat[Text, IntWritable]])

    // Set input and output paths
    FileInputFormat.addInputPath(job, new Path(inputPath))
    FileOutputFormat.setOutputPath(job, outputDir)

    // Run the job
    job.waitForCompletion(true)
  }
}
























//package mapreduce
//
//import org.apache.hadoop.conf.Configuration
//import org.apache.hadoop.fs.{FileSystem, Path}
//import org.apache.hadoop.io.{IntWritable, LongWritable, Text}
//import org.apache.hadoop.mapreduce.Job
//import org.apache.hadoop.mapreduce.lib.input.{FileInputFormat, TextInputFormat}
//import org.apache.hadoop.mapreduce.lib.output.{FileOutputFormat, TextOutputFormat}
//
//import java.io.{BufferedWriter, FileWriter, IOException}
//import scala.io.Source
//
//class TokenFrequencyJob {
//
//
//  def runJob(inputPath: String, outputPath: String): Boolean = {
//    val conf = new Configuration()
//    val job = Job.getInstance(conf, "Token Frequency with Tokens")
//    job.setJarByClass(classOf[TokenFrequencyJob])
//
//    // Set Mapper and Reducer
//    job.setMapperClass(classOf[TokenFrequencyMapper])
//    job.setReducerClass(classOf[TokenFrequencyReducer])
//
//    // Set output key and value types
//    job.setOutputKeyClass(classOf[Text])
//    job.setOutputValueClass(classOf[IntWritable])
//
//    // Set input and output formats
//    job.setInputFormatClass(classOf[TextInputFormat])
//    job.setOutputFormatClass(classOf[TextOutputFormat[Text, IntWritable]])
//
//    // Set input and output paths
//    FileInputFormat.addInputPath(job, new Path(inputPath))
//    FileOutputFormat.setOutputPath(job, new Path(outputPath))
//
//    job.waitForCompletion(true)
//  }
//}
//
////object TokenFrequencyJob {
////  def main(args: Array[String]): Unit = {
////    if (args.length < 2) {
////      println("Usage: TokenFrequencyJob <input path> <output path>")
////      System.exit(1)
////    }
////
////    val inputPath = args(0)
////    val outputPath = args(1)
////
////    val job = new TokenFrequencyJob()
////    job.runJob(inputPath, outputPath)
////  }
////}
