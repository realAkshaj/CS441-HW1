import config.Configuration.{numShards, sampleText, testingText}
import data.{DataCleaner, Sharder}
import mapreduce.{CosineSimilarityJob, EmbeddingJob, TokenFrequencyJob}

import java.io.{BufferedWriter, File, FileWriter, IOException, InputStream, OutputStreamWriter, PrintWriter}
import java.nio.charset.StandardCharsets
import java.nio.file.{Files, Paths, StandardOpenOption}
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.hadoop.io.{DoubleWritable, Text}
import org.apache.hadoop.mapreduce.Job
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat
import org.apache.hadoop.mapreduce.lib.output.{FileOutputFormat, TextOutputFormat}
import data.DataCleaner
import data.Sharder
//import embedding.VectorEmbeddingGenerator

import scala.io.Source


object MainApp {
  def main(args: Array[String]): Unit = {

    // 1. Data Cleaning and Sharding
    val inputFilePath = "hdfs:///input/testing_text.txt" // Update this path to your HDFS input file
    val numShards = 5 // Number of shards (adjust as needed)

    // Setup Hadoop configuration and FileSystem
    val conf = new Configuration()
    val fs = FileSystem.get(conf)

    // Read and clean the input text from HDFS
    val inputFile = fs.open(new Path(inputFilePath))
    val testingText = Source.fromInputStream(inputFile).mkString
    inputFile.close()

    // Perform cleaning and sharding as before
    val cleanedText = DataCleaner.cleanText(testingText)
    val shards = Sharder.shardText(cleanedText, numShards)

    // Write the shards to the HDFS output file
    val shardFilePath = "hdfs:///output/sharded_text.txt"
    writeToHDFS(shardFilePath, shards.mkString("\n"), fs)
    println(s"Sharded text written to: $shardFilePath")

    // 2. Run Token Frequency MapReduce Job
    val tokenOutputPath = "hdfs:///output/tokenization_output"
    val tokenJob = new TokenFrequencyJob()
    if (tokenJob.runJob(shardFilePath, tokenOutputPath)) {
      println(s"Token MapReduce job completed successfully. Check the output at: $tokenOutputPath")

      // 3. Check Embedding Input in HDFS
      val embeddingInputPath = "hdfs:///output/tokens_output_only.txt"
      if (!fs.exists(new Path(embeddingInputPath))) {
        println(s"Embedding input file not found: $embeddingInputPath. Terminating process.")
        return
      }

      // 4. Run Embedding MapReduce Job
      lazy val embeddingOutputPath = "hdfs:///output/embedding_output"
      val embeddingJob = new EmbeddingJob()

      if (embeddingJob.runJob(embeddingInputPath, embeddingOutputPath)) {

        println(s"Embedding MapReduce job completed successfully. Check the output at: $embeddingOutputPath")

        // 5. Run Cosine Similarity Job
        val cosineInputPath = "hdfs:///output/embedding_output/part-r-00000" // The embeddings generated from the previous job
        val cosineOutputPath = "hdfs:///output/cosine_similarity_output"
        val cosineJob = new CosineSimilarityJob()

        if (cosineJob.runJob(cosineInputPath, cosineOutputPath)) {
          println(s"Cosine Similarity MapReduce job completed successfully. Check the output at: $cosineOutputPath")
          lazy val tokenFilePath = "hdfs:///output/tokenization_output/part-r-00000"
          lazy val embeddingFilePath = "hdfs:///output/embedding_output/part-r-00000"
          lazy val cosineFilePath = "hdfs:///output/cosine_similarity_output/part-r-00000"
          lazy val csvFilePath = "hdfs:///output/final_output.csv"

          val tokenFrequencyMap = readTokenFile(tokenFilePath, fs)
          val embeddingMap = readEmbeddingFile(embeddingFilePath, fs)
          val cosineSimilarityMap = readCosineFile(cosineFilePath, fs)

          writeToCSV(tokenFrequencyMap, embeddingMap, cosineSimilarityMap, csvFilePath, fs)
          println(s"CSV file created at: $csvFilePath")

        } else {
          println("Cosine Similarity MapReduce job failed.")
        }

      } else {
        println("Embedding MapReduce job failed.")
      }

    } else {
      println("Token MapReduce job failed.")
    }

    // 6. Create Final CSV Output from HDFS

  }

  // Helper function to write to HDFS
  def writeToHDFS(filePath: String, content: String, fs: FileSystem): Unit = {
    val path = new Path(filePath)
    if (fs.exists(path)) {
      fs.delete(path, true)
    }
    val outputStream = fs.create(path)
    outputStream.write(content.getBytes(StandardCharsets.UTF_8))
    outputStream.close()
  }

  // Helper function to read from HDFS token file
  def readTokenFile(tokenFilePath: String, fs: FileSystem): Map[Int, (String, Int)] = {
    val inputFile = fs.open(new Path(tokenFilePath))
    val lines = Source.fromInputStream(inputFile).getLines().foldLeft(Map[Int, (String, Int)]()) { (map, line) =>
      val parts = line.split("\\s*,\\s*")
      val word = parts(0).trim
      val token = parts(1).split(",")(0).trim.toInt
      val frequency = parts(2).trim.toInt
      map.updated(token, (word, frequency))
    }
    inputFile.close()
    lines
  }

  def readEmbeddingFile(embeddingFilePath: String, fs: FileSystem): Map[Int, String] = {
    val inputFile = fs.open(new Path(embeddingFilePath))
    val lines = Source.fromInputStream(inputFile).getLines().foldLeft(Map[Int, String]()) { (map, line) =>
      val parts = line.split("\\s+")
      val token = parts(0).toInt
      val embedding = parts.drop(1).mkString(",")
      map.updated(token, embedding)
    }
    inputFile.close()
    lines
  }

  def readCosineFile(cosineFilePath: String, fs: FileSystem): Map[Int, String] = {
    val inputFile = fs.open(new Path(cosineFilePath))
    val lines = Source.fromInputStream(inputFile).getLines().foldLeft(Map[Int, String]()) { (map, line) =>
      val parts = line.split("\\s+")
      val token = parts(0).toInt
      val closestWords = parts.drop(1).mkString(" ")
      map.updated(token, closestWords)
    }
    inputFile.close()
    lines
  }

  // Helper function to write the CSV to HDFS
  def writeToCSV(
                  tokenFrequencyMap: Map[Int, (String, Int)],
                  embeddingMap: Map[Int, String],
                  cosineSimilarityMap: Map[Int, String],
                  csvFilePath: String,
                  fs: FileSystem
                ): Unit = {
    val outputPath = new Path(csvFilePath)
    val outputStream = fs.create(outputPath, true)
    val writer = new BufferedWriter(new OutputStreamWriter(outputStream, StandardCharsets.UTF_8))

    writer.write("Word,Token,Frequency,Embeddings,Closest Similar Word\n")

    for ((token, (word, frequency)) <- tokenFrequencyMap) {
      val embedding = embeddingMap.getOrElse(token, "N/A")
      val closestWords = cosineSimilarityMap.getOrElse(token, "N/A")
      writer.write(s"$word,$token,$frequency,$embedding,$closestWords\n")
    }

    writer.close()
  }
}


//
//    // 1. Data Cleaning and Sharding
//    val inputFilePath = "src/main/resources/input/testing_text.txt" // Update this path to your input file
//    val numShards = 5 // Number of shards (adjust as needed)
//    // Read and clean the input text
//    val testingText = new String(Files.readAllBytes(Paths.get(inputFilePath)), StandardCharsets.UTF_8)
//    val cleanedText = DataCleaner.cleanText(testingText)
//    // Shard the text
//    val shards = Sharder.shardText(cleanedText, numShards)
//
//    val tokenFilePath = "resources/output/tokenization_output/part-r-00000"
//    val embeddingFilePath = "resources/output/embedding_output/part-r-00000"
//    val cosineFilePath = "resources/output/cosine_similarity_output/part-r-00000"
//    val csvFilePath = "resources/output/final_output.csv"
//
//    // Write the shards to the output file
//    val outputDir = "resources/output"
//    val shardFilePath = s"$outputDir/sharded_text.txt"
//    writeToFile(shardFilePath, shards.mkString("\n"))
//    println(s"Sharded text written to: $shardFilePath")
//
//    // 2. Run Token Frequency MapReduce Job
//    val tokenOutputPath = "resources/output/tokenization_output"
//    val tokenJob = new TokenFrequencyJob()
//    if (tokenJob.runJob(shardFilePath, tokenOutputPath)) {
//      println(s"Token MapReduce job completed successfully. Check the output at: $tokenOutputPath")
//
//      // 3. Ensure tokens_output_only.txt exists and is valid
//      val embeddingInputPath = s"resources/output/tokens_output_only.txt"
//      if (!Files.exists(Paths.get(embeddingInputPath))) {
//        println(s"Embedding input file not found: $embeddingInputPath. Terminating process.")
//        return
//      }
//
//      // Check if file is empty
//      if (Files.size(Paths.get(embeddingInputPath)) == 0) {
//        println(s"Embedding input file is empty: $embeddingInputPath. Terminating process.")
//        return
//      }
//
//      println(s"Embedding input file found: $embeddingInputPath")
//
//      // 4. Run Embedding MapReduce Job
//      val embeddingOutputPath = "resources/output/embedding_output"
//      val embeddingJob = new EmbeddingJob()
//
//      if (embeddingJob.runJob(embeddingInputPath, embeddingOutputPath)) {
//        println(s"Embedding MapReduce job completed successfully. Check the output at: $embeddingOutputPath")
//        val cosineInputPath = embeddingOutputPath // The embeddings generated from the previous job
//        val cosineOutputPath = "resources/output/cosine_similarity_output"
//        val cosineJob = new CosineSimilarityJob()
//
//        if (cosineJob.runJob(cosineInputPath, cosineOutputPath)) {
//          println(s"Cosine Similarity MapReduce job completed successfully. Check the output at: $cosineOutputPath")
//        } else {
//          println("Cosine Similarity MapReduce job failed.")
//        }
//
//      } else {
//        println("Embedding MapReduce job failed.")
//      }
//
//    } else {
//      println("Token MapReduce job failed.")
//    }
//    val tokenFrequencyMap = readTokenFile(tokenFilePath)
//    val embeddingMap = readEmbeddingFile(embeddingFilePath)
//    val cosineSimilarityMap = readCosineFile(cosineFilePath)
//    writeToCSV(tokenFrequencyMap, embeddingMap, cosineSimilarityMap, csvFilePath)
//    println(s"CSV file created at: $csvFilePath")
//
//  }


//  // Helper function to write to a file
//  def writeToFile(filePath: String, content: String): Unit = {
//    try {
//      val path = Paths.get(filePath)
//      Files.createDirectories(path.getParent) // Create directories if they don't exist
//      Files.write(path, content.getBytes(StandardCharsets.UTF_8))
//    } catch {
//      case e: IOException =>
//        println(s"Error writing to file: $filePath. Error: ${e.getMessage}")
//    }
//  }
//
//  def readTokenFile(tokenFilePath: String): Map[Int, (String, Int)] = {
//    Source.fromFile(tokenFilePath).getLines().map { line =>
//      val parts = line.split("\\s*,\\s*") // Split by commas with possible whitespace
//      val word = parts(0).trim
//      val token = parts(1).split(",")(0).trim.toInt // Convert the first token part to an integer
//      val frequency = parts(2).trim.toInt
//      token -> (word, frequency)
//    }.toMap
//  }
//
//  def readEmbeddingFile(embeddingFilePath: String): Map[Int, String] = {
//    Source.fromFile(embeddingFilePath).getLines().map { line =>
//      val parts = line.split("\\s+")
//      val token = parts(0).toInt
//      val embedding = parts(1)
//      token -> embedding
//    }.toMap
//  }
//
//  def readCosineFile(cosineFilePath: String): Map[Int, String] = {
//    Source.fromFile(cosineFilePath).getLines().map { line =>
//      val parts = line.split("\\s+")
//      val token = parts(0).toInt
//      val closestWords = parts.drop(1).mkString(" ")
//      token -> closestWords
//    }.toMap
//  }
//
//  def writeToCSV(
//                  tokenFrequencyMap: Map[Int, (String, Int)],
//                  embeddingMap: Map[Int, String],
//                  cosineSimilarityMap: Map[Int, String],
//                  csvFilePath: String
//                ): Unit = {
//    val writer = new BufferedWriter(new FileWriter(csvFilePath))
//    writer.write("Word,Token,Frequency,Embeddings,Closest Similar Word\n")
//
//    for ((token, (word, frequency)) <- tokenFrequencyMap) {
//      val embedding = embeddingMap.getOrElse(token, "N/A")
//      val closestWords = cosineSimilarityMap.getOrElse(token, "N/A")
//      writer.write(s"$word,$token,$frequency,$embedding,$closestWords\n")
//    }
//
//    writer.close()
//  }
//
//
//}




