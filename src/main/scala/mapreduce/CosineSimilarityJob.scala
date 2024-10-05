package mapreduce

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.hadoop.io.Text
import org.apache.hadoop.mapreduce.Job
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat
import org.slf4j.LoggerFactory

class CosineSimilarityJob {

  private val logger = LoggerFactory.getLogger(classOf[CosineSimilarityJob])

  def runJob(inputPath: String, outputPath: String): Boolean = {
    try {
      val conf = new Configuration()
      val job = Job.getInstance(conf, "Cosine Similarity")
      // Check if output path exists
      val outputDir = new Path(outputPath)

      // Check if output path exists, and delete it if it does
      val fs = FileSystem.get(conf)
      if (fs.exists(outputDir)) {
        println(s"Output path $outputPath already exists, deleting it.")
        fs.delete(outputDir, true) // true indicates recursive deletion
      }


      job.setJarByClass(classOf[CosineSimilarityJob])

      // Set the mapper, reducer, and the output types
      job.setMapperClass(classOf[CosineSimilarityMapper])
      job.setReducerClass(classOf[CosineSimilarityReducer])

      job.setOutputKeyClass(classOf[Text])
      job.setOutputValueClass(classOf[Text])

      // Set the input and output paths
      FileInputFormat.addInputPath(job, new Path(inputPath))
      FileOutputFormat.setOutputPath(job, new Path(outputPath))

      // Submit the job and wait for it to complete
      val success = job.waitForCompletion(true)
      success
    } catch {
      case e: Exception =>
        logger.error("Job execution failed: ", e)
        false
    }
  }
}

