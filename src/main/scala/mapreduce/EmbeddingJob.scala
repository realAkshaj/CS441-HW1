package mapreduce

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.hadoop.io.Text
import org.apache.hadoop.mapreduce.Job
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat

class EmbeddingJob {
  def runJob(inputPath: String, outputPath: String): Boolean = {
    val conf = new Configuration()
    val fs = FileSystem.get(conf)

    // Delete the output path if it already exists
    val outputDir = new Path(outputPath)
    if (fs.exists(outputDir)) {
      fs.delete(outputDir, true)
    }

    // Configure the MapReduce job
    val job = Job.getInstance(conf, "Embedding Generator")
    job.setJarByClass(classOf[EmbeddingMapper])

    // Set mapper and reducer classes
    job.setMapperClass(classOf[EmbeddingMapper])
    job.setReducerClass(classOf[EmbeddingReducer])

    // Set output key and value types
    job.setOutputKeyClass(classOf[Text])
    job.setOutputValueClass(classOf[Text])

    // Set input and output paths
    FileInputFormat.addInputPath(job, new Path(inputPath))
    FileOutputFormat.setOutputPath(job, new Path(outputPath))

    // Submit the job and wait for completion
    if (job.waitForCompletion(true)) {
      true
    }
    else {
      false
    }
  }
}