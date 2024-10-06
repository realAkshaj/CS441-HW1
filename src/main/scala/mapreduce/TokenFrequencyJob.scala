package mapreduce

import mapreduce.{TokenFrequencyMapper, TokenFrequencyReducer}
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.hadoop.io.{IntWritable, NullWritable, Text}
import org.apache.hadoop.mapreduce.Job
import org.apache.hadoop.mapreduce.lib.input.{FileInputFormat, TextInputFormat}
import org.apache.hadoop.mapreduce.lib.output.{FileOutputFormat, TextOutputFormat}

import java.nio.file.{Files, Paths}
import java.util.Comparator


class TokenFrequencyJob {

  def runJob(inputPath: String, outputPath: String): Boolean = {
    val conf = new Configuration()

    // Check if the output path already exists and delete it if necessary
    val outputDir = Paths.get(outputPath)
    if (Files.exists(outputDir)) {
      Files.walk(outputDir).sorted(Comparator.reverseOrder()).forEach(Files.deleteIfExists)
    }

    // Create a new job instance
    val job = Job.getInstance(conf, "Word-Token-Frequency")
    job.setJarByClass(classOf[TokenFrequencyJob])

    // Set Mapper class to emit word-token pairs
    job.setMapperClass(classOf[TokenFrequencyMapper])

    // Set Reducer class to write word-token-frequency and tokens file
    job.setReducerClass(classOf[TokenFrequencyReducer])

    // Set the output key and value types
    job.setOutputKeyClass(classOf[Text])
    job.setOutputValueClass(classOf[Text])

    // Set input and output formats
    job.setInputFormatClass(classOf[TextInputFormat])
    job.setOutputFormatClass(classOf[TextOutputFormat[Text, Text]])

    // Set input and output paths
    FileInputFormat.addInputPath(job, new Path(inputPath))
    FileOutputFormat.setOutputPath(job, new Path(outputPath))

    // Run the job and return true if it completes successfully
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
