import data.{DataCleaner, Sharder}
import mapreduce.{CosineSimilarityJob, EmbeddingJob, TokenFrequencyJob}

import java.nio.file.{Files, Paths}
import java.nio.charset.StandardCharsets
import scala.io.Source

object MainApp {
  def main(args: Array[String]): Unit = {

    // 1. Data Cleaning and Sharding
    val inputFilePath = "src/main/resources/input/testing_text.txt" // Update this to a local file path
    val numShards = 5 // Number of shards (adjust as needed)

    // Read and clean the input text from the local file system
    val testingText = Source.fromFile(inputFilePath).mkString

    // Perform cleaning and sharding as before
    val cleanedText = DataCleaner.cleanText(testingText)
    val shards = Sharder.shardText(cleanedText, numShards)
    println(s"Number of shards created: ${shards.length}")
    shards.zipWithIndex.foreach { case (shard, index) =>
      println(s"Shard $index: $shard")
    }
    // Write the shards to the local output file
    val shardFilePath = "resources/output/sharded_text.txt"
    writeToLocal(shardFilePath, shards.mkString("\n"))
    println(s"Sharded text written to: $shardFilePath")

    // 2. Run Token Frequency MapReduce Job (locally)
    val tokenOutputPath = "resources/output/tokenization_output"
    val tokenJob = new TokenFrequencyJob()
    if (tokenJob.runJob(shardFilePath, tokenOutputPath)) {
      println(s"Token MapReduce job completed successfully. Check the output at: $tokenOutputPath")

      // 3. Check Embedding Input in the local file system
      val embeddingInputPath = "resources/output/onlytokens.txt"
      if (Files.exists(Paths.get(embeddingInputPath))) {
        println(s"Embedding input file exists at: $embeddingInputPath")

        // 4. Run Embedding MapReduce Job (locally)
        val embeddingOutputPath = "resources/output/embedding_output"
        val embeddingJob = new EmbeddingJob()
        //Thread.sleep(5000)
        if (embeddingJob.runJob(embeddingInputPath, embeddingOutputPath)) {
          println(s"Embedding MapReduce job completed successfully. Check the output at: $embeddingOutputPath")

          // 5. Run Cosine Similarity Job (locally)
          val cosineInputPath = "resources/output/embedding_output/part-r-00000"
          val cosineOutputPath = "resources/output/cosine-similarity-output"
          val cosineJob = new CosineSimilarityJob()

          if (cosineJob.runJob(cosineInputPath, cosineOutputPath)) {
            println(s"Cosine Similarity MapReduce job completed successfully. Check the output at: $cosineOutputPath")

            // Generate final CSV output
            val tokenFilePath = "resources/output/tokenization_output/part-r-00000"
            val embeddingFilePath = "resources/output/embedding_output/part-r-00000"
            val cosineFilePath = "resources/output/cosine-similarity-output/part-r-00000"
            val csvFilePath = "resources/output/final_output.csv"

            val tokenFrequencyMap = readTokenFile(tokenFilePath)
            val embeddingMap = readEmbeddingFile(embeddingFilePath)
            val cosineSimilarityMap = readCosineFile(cosineFilePath)

            writeToCSV(tokenFrequencyMap, embeddingMap, cosineSimilarityMap, csvFilePath)
            println(s"CSV file created at: $csvFilePath")

          } else {
            println("Cosine Similarity MapReduce job failed.")
          }
        } else {
          println("Embedding MapReduce job failed.")
        }

      } else {
        println(s"Error: Embedding input file not found or could not be opened: $embeddingInputPath.")
      }

    } else {
      println("Token MapReduce job failed.")
    }

    // 6. Create Final CSV Output from local file system
  }

  // Helper function to write to local file system
  def writeToLocal(filePath: String, content: String): Unit = {
    val path = Paths.get(filePath)
    if (Files.exists(path)) {
      Files.delete(path)
    }
    Files.write(path, content.getBytes(StandardCharsets.UTF_8))
  }

  // Helper function to read from local token file
  def readTokenFile(tokenFilePath: String): Map[Int, (String, Int)] = {
    // Open the local file using scala.io.Source
    val inputFile = Source.fromFile(tokenFilePath)

    val lines = inputFile.getLines().foldLeft(Map[Int, (String, Int)]()) { (map, line) =>
      // Split by ' - ' instead of ','
      val parts = line.split("\\s*-\\s*")

      // Check if parts has at least 3 elements (word, token, frequency)
      if (parts.length == 3) {
        val word = parts(0).trim
        val token = parts(1).trim.toInt
        val frequency = parts(2).trim.toInt
        map.updated(token, (word, frequency))
      } else {
        println(s"Warning: Invalid line format in line: $line")
        map
      }
    }

    inputFile.close()
    lines
  }

  def readEmbeddingFile(embeddingFilePath: String): Map[Int, String] = {
    val lines = Source.fromFile(embeddingFilePath).getLines().foldLeft(Map[Int, String]()) { (map, line) =>
      val parts = line.split("\\s+")
      val token = parts(0).toInt
      val embedding = parts.drop(1).mkString(",")
      map.updated(token, embedding)
    }
    lines
  }

  def readCosineFile(cosineFilePath: String): Map[Int, String] = {
    val lines = Source.fromFile(cosineFilePath).getLines().foldLeft(Map[Int, String]()) { (map, line) =>
      val parts = line.split("\\s+")
      val token = parts(0).toInt
      val closestWords = parts.drop(1).mkString(" ")
      map.updated(token, closestWords)
    }
    lines
  }

  // Helper function to write the CSV to local file system
  def writeToCSV(
                  tokenFrequencyMap: Map[Int, (String, Int)],
                  embeddingMap: Map[Int, String],
                  cosineSimilarityMap: Map[Int, String],
                  csvFilePath: String
                ): Unit = {
    val path = Paths.get(csvFilePath)
    val writer = Files.newBufferedWriter(path, StandardCharsets.UTF_8)

    writer.write("Word,Token,Frequency,Embeddings,Closest Similar Word\n")

    for ((token, (word, frequency)) <- tokenFrequencyMap) {
      val embedding = embeddingMap.getOrElse(token, "N/A")
      val closestWords = cosineSimilarityMap.getOrElse(token, "N/A")
      writer.write(s"$word,$token,$frequency,$embedding,$closestWords\n")
    }

    writer.close()
  }
}

















//import config.Configuration.{numShards, sampleText, testingText}
//import data.{DataCleaner, Sharder}
//import mapreduce.{CosineSimilarityJob, EmbeddingJob, TokenFrequencyJob}
//
//import java.io.{BufferedWriter, File, FileWriter, IOException, InputStream, OutputStreamWriter, PrintWriter}
//import java.nio.charset.StandardCharsets
//import java.nio.file.{Files, Paths, StandardOpenOption}
//import org.apache.hadoop.conf.Configuration
//import org.apache.hadoop.fs.{FileSystem, Path}
//import org.apache.hadoop.io.{DoubleWritable, Text}
//import org.apache.hadoop.mapreduce.Job
//import org.apache.hadoop.mapreduce.lib.input.FileInputFormat
//import org.apache.hadoop.mapreduce.lib.output.{FileOutputFormat, TextOutputFormat}
//import data.DataCleaner
//import data.Sharder
////import embedding.VectorEmbeddingGenerator
//
//import scala.io.Source
//
//
//object MainApp {
//  def main(args: Array[String]): Unit = {
//
//    // 1. Data Cleaning and Sharding
//    val inputFilePath = "hdfs:///user/hadoop/input/testing_text.txt" // Update this path to your HDFS input file
//    val numShards = 5 // Number of user/hadoop/f shards (adjust as needed)
//
//    // Setup Hadoop configuration and FileSystem
//    val conf = new Configuration()
//    val fs = FileSystem.get(conf)
//
//    // Read and clean the input text from HDFS
//    val inputFile = fs.open(new Path(inputFilePath))
//    val testingText = Source.fromInputStream(inputFile).mkString
//    inputFile.close()
//
//    // Perform cleaning and sharding as before
//    val cleanedText = DataCleaner.cleanText(testingText)
//    val shards = Sharder.shardText(cleanedText, numShards)
//
//    // Write the shards to the HDFS output file
//    val shardFilePath = "hdfs:///home/hadoop/output/sharded_text.txt"
//    writeToHDFS(shardFilePath, shards.mkString("\n"), fs)
//    println(s"Sharded text written to: $shardFilePath")
//
//    // 2. Run Token Frequency MapReduce Job
//    val tokenOutputPath = "hdfs:///user/hadoop/output/tokenization_output"
//    val tokenJob = new TokenFrequencyJob()
//    if (tokenJob.runJob(shardFilePath, tokenOutputPath)) {
//      println(s"Token MapReduce job completed successfully. Check the output at: $tokenOutputPath")
//      //      Thread.sleep(5000)
//      // 3. Check Embedding Input in HDFS
//      val embeddingInputPath = "hdfs:///user/hadoop/tokens_output_only.txt"
//      try {
//        val input = fs.open(new Path(embeddingInputPath)) // Try to open the file directly
//        input.close() // If opened, close it
//        println(s"Embedding input file exists at: $embeddingInputPath")
//
//        // 4. Run Embedding MapReduce Job
//        val embeddingOutputPath = "hdfs:///user/hadoop/output/embedding_output"
//        val embeddingJob = new EmbeddingJob()
//        Thread.sleep(5000)
//        if (embeddingJob.runJob(embeddingInputPath, embeddingOutputPath)) {
//          println(s"Embedding MapReduce job completed successfully. Check the output at: $embeddingOutputPath")
//
//          // 5. Run Cosine Similarity Job
//          val cosineInputPath = "hdfs:///user/hadoop/output/embedding_output/part-r-00000"
//          val cosineOutputPath = "hdfs:///user/hadoop/output/cosine_similarity_output"
//          val cosineJob = new CosineSimilarityJob()
//
//          if (cosineJob.runJob(cosineInputPath, cosineOutputPath)) {
//            println(s"Cosine Similarity MapReduce job completed successfully. Check the output at: $cosineOutputPath")
//
//            // Generate final CSV output
//            val tokenFilePath = "hdfs:///user/hadoop/output/tokenization_output/part-r-00000"
//            val embeddingFilePath = "hdfs:///user/hadoop/output/embedding_output/part-r-00000"
//            val cosineFilePath = "hdfs:///user/hadoop/output/cosine_similarity_output/part-r-00000"
//            val csvFilePath = "hdfs:///user/hadoop/output/final_output.csv"
//
//            val tokenFrequencyMap = readTokenFile(tokenFilePath, fs)
//            val embeddingMap = readEmbeddingFile(embeddingFilePath, fs)
//            val cosineSimilarityMap = readCosineFile(cosineFilePath, fs)
//
//            writeToCSV(tokenFrequencyMap, embeddingMap, cosineSimilarityMap, csvFilePath, fs)
//            println(s"CSV file created at: $csvFilePath")
//
//          } else {
//            println("Cosine Similarity MapReduce job failed.")
//          }
//        } else {
//          println("Embedding MapReduce job failed.")
//        }
//
//      } catch {
//        case e: IOException =>
//          println(s"Error: Embedding input file not found or could not be opened: $embeddingInputPath. Error: ${e.getMessage}")
//      }
//
//    } else {
//      println("Token MapReduce job failed.")
//    }
//
//    // 6. Create Final CSV Output from HDFS
//
//  }
//
//  // Helper function to write to HDFS
//  def writeToHDFS(filePath: String, content: String, fs: FileSystem): Unit = {
//    val path = new Path(filePath)
//    if (fs.exists(path)) {
//      fs.delete(path, true)
//    }
//    val outputStream = fs.create(path)
//    outputStream.write(content.getBytes(StandardCharsets.UTF_8))
//    outputStream.close()
//  }
//
//  // Helper function to read from HDFS token file
//  def readTokenFile(tokenFilePath: String, fs: FileSystem): Map[Int, (String, Int)] = {
//    val inputFile = fs.open(new Path(tokenFilePath))
//    val lines = Source.fromInputStream(inputFile).getLines().foldLeft(Map[Int, (String, Int)]()) { (map, line) =>
//      val parts = line.split("\\s*,\\s*")
//      val word = parts(0).trim
//      val token = parts(1).split(",")(0).trim.toInt
//      val frequency = parts(2).trim.toInt
//      map.updated(token, (word, frequency))
//    }
//    inputFile.close()
//    lines
//  }
//
//  def readEmbeddingFile(embeddingFilePath: String, fs: FileSystem): Map[Int, String] = {
//    val inputFile = fs.open(new Path(embeddingFilePath))
//    val lines = Source.fromInputStream(inputFile).getLines().foldLeft(Map[Int, String]()) { (map, line) =>
//      val parts = line.split("\\s+")
//      val token = parts(0).toInt
//      val embedding = parts.drop(1).mkString(",")
//      map.updated(token, embedding)
//    }
//    inputFile.close()
//    lines
//  }
//
//  def readCosineFile(cosineFilePath: String, fs: FileSystem): Map[Int, String] = {
//    val inputFile = fs.open(new Path(cosineFilePath))
//    val lines = Source.fromInputStream(inputFile).getLines().foldLeft(Map[Int, String]()) { (map, line) =>
//      val parts = line.split("\\s+")
//      val token = parts(0).toInt
//      val closestWords = parts.drop(1).mkString(" ")
//      map.updated(token, closestWords)
//    }
//    inputFile.close()
//    lines
//  }
//
//  // Helper function to write the CSV to HDFS
//  def writeToCSV(
//                  tokenFrequencyMap: Map[Int, (String, Int)],
//                  embeddingMap: Map[Int, String],
//                  cosineSimilarityMap: Map[Int, String],
//                  csvFilePath: String,
//                  fs: FileSystem
//                ): Unit = {
//    val outputPath = new Path(csvFilePath)
//    val outputStream = fs.create(outputPath, true)
//    val writer = new BufferedWriter(new OutputStreamWriter(outputStream, StandardCharsets.UTF_8))
//
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
//}
////  def main(args: Array[String]): Unit = {
////
////    // 1. Data Cleaning and Sharding
////    val inputFilePath = "hdfs:///user/hadoop/input/testing_text.txt" // Update this path to your HDFS input file
////    val numShards = 5 // Number ouser/hadoop/f shards (adjust as needed)
////
////    // Setup Hadoop configuration and FileSystem
////    val conf = new Configuration()
////    val fs = FileSystem.get(conf)
////
////    // Read and clean the input text from HDFS
////    val inputFile = fs.open(new Path(inputFilePath))
////    val testingText = Source.fromInputStream(inputFile).mkString
////    inputFile.close()
////
////    // Perform cleaning and sharding as before
////    val cleanedText = DataCleaner.cleanText(testingText)
////    val shards = Sharder.shardText(cleanedText, numShards)
////
////    // Write the shards to the HDFS output file
////    val shardFilePath = "hdfs:///user/hadoop/output/sharded_text.txt"
////    writeToHDFS(shardFilePath, shards.mkString("\n"), fs)
////    println(s"Sharded text written to: $shardFilePath")
////
////    // 2. Run Token Frequency MapReduce Job
////    val tokenOutputPath = "hdfs:///user/hadoop/output/tokenization_output"
////    val tokenJob = new TokenFrequencyJob()
////    if (tokenJob.runJob(shardFilePath, tokenOutputPath)) {
////      println(s"Token MapReduce job completed successfully. Check the output at: $tokenOutputPath")
//////      Thread.sleep(5000)
////      // 3. Check Embedding Input in HDFS
////      val embeddingInputPath = "hdfs:///user/hadoop/output/tokens_output_only.txt"
////      try {
////        val input = fs.open(new Path(embeddingInputPath)) // Try to open the file directly
////        input.close() // If opened, close it
////        println(s"Embedding input file exists at: $embeddingInputPath")
////
////        // 4. Run Embedding MapReduce Job
////        val embeddingOutputPath = "hdfs:///user/hadoop/output/embedding_output"
////        val embeddingJob = new EmbeddingJob()
////
////        if (embeddingJob.runJob(embeddingInputPath, embeddingOutputPath)) {
////          println(s"Embedding MapReduce job completed successfully. Check the output at: $embeddingOutputPath")
////
////          // 5. Run Cosine Similarity Job
////          val cosineInputPath = "hdfs:///user/hadoop/output/embedding_output/part-r-00000"
////          val cosineOutputPath = "hdfs:///user/hadoop/output/cosine_similarity_output"
////          val cosineJob = new CosineSimilarityJob()
////
////          if (cosineJob.runJob(cosineInputPath, cosineOutputPath)) {
////            println(s"Cosine Similarity MapReduce job completed successfully. Check the output at: $cosineOutputPath")
////
////            // Generate final CSV output
////            val tokenFilePath = "hdfs:///user/hadoop/output/tokenization_output/part-r-00000"
////            val embeddingFilePath = "hdfs:///user/hadoop/output/embedding_output/part-r-00000"
////            val cosineFilePath = "hdfs:///user/hadoop/output/cosine_similarity_output/part-r-00000"
////            val csvFilePath = "hdfs:///user/hadoop/output/final_output.csv"
////
////            val tokenFrequencyMap = readTokenFile(tokenFilePath, fs)
////            val embeddingMap = readEmbeddingFile(embeddingFilePath, fs)
////            val cosineSimilarityMap = readCosineFile(cosineFilePath, fs)
////
////            writeToCSV(tokenFrequencyMap, embeddingMap, cosineSimilarityMap, csvFilePath, fs)
////            println(s"CSV file created at: $csvFilePath")
////
////          } else {
////            println("Cosine Similarity MapReduce job failed.")
////          }
////        } else {
////          println("Embedding MapReduce job failed.")
////        }
////
////      } catch {
////        case e: IOException =>
////          println(s"Error: Embedding input file not found or could not be opened: $embeddingInputPath. Error: ${e.getMessage}")
////      }
////
////    } else {
////      println("Token MapReduce job failed.")
////    }
////
////    // 6. Create Final CSV Output from HDFS
////
////  }
////
////  // Helper function to write to HDFS
////  def writeToHDFS(filePath: String, content: String, fs: FileSystem): Unit = {
////    val path = new Path(filePath)
////    if (fs.exists(path)) {
////      fs.delete(path, true)
////    }
////    val outputStream = fs.create(path)
////    outputStream.write(content.getBytes(StandardCharsets.UTF_8))
////    outputStream.close()
////  }
////
////  // Helper function to read from HDFS token file
////  def readTokenFile(tokenFilePath: String, fs: FileSystem): Map[Int, (String, Int)] = {
////    val inputFile = fs.open(new Path(tokenFilePath))
////    val lines = Source.fromInputStream(inputFile).getLines().foldLeft(Map[Int, (String, Int)]()) { (map, line) =>
////      val parts = line.split("\\s*,\\s*")
////      val word = parts(0).trim
////      val token = parts(1).split(",")(0).trim.toInt
////      val frequency = parts(2).trim.toInt
////      map.updated(token, (word, frequency))
////    }
////    inputFile.close()
////    lines
////  }
////
////  def readEmbeddingFile(embeddingFilePath: String, fs: FileSystem): Map[Int, String] = {
////    val inputFile = fs.open(new Path(embeddingFilePath))
////    val lines = Source.fromInputStream(inputFile).getLines().foldLeft(Map[Int, String]()) { (map, line) =>
////      val parts = line.split("\\s+")
////      val token = parts(0).toInt
////      val embedding = parts.drop(1).mkString(",")
////      map.updated(token, embedding)
////    }
////    inputFile.close()
////    lines
////  }
////
////  def readCosineFile(cosineFilePath: String, fs: FileSystem): Map[Int, String] = {
////    val inputFile = fs.open(new Path(cosineFilePath))
////    val lines = Source.fromInputStream(inputFile).getLines().foldLeft(Map[Int, String]()) { (map, line) =>
////      val parts = line.split("\\s+")
////      val token = parts(0).toInt
////      val closestWords = parts.drop(1).mkString(" ")
////      map.updated(token, closestWords)
////    }
////    inputFile.close()
////    lines
////  }
////
////  // Helper function to write the CSV to HDFS
////  def writeToCSV(
////                  tokenFrequencyMap: Map[Int, (String, Int)],
////                  embeddingMap: Map[Int, String],
////                  cosineSimilarityMap: Map[Int, String],
////                  csvFilePath: String,
////                  fs: FileSystem
////                ): Unit = {
////    val outputPath = new Path(csvFilePath)
////    val outputStream = fs.create(outputPath, true)
////    val writer = new BufferedWriter(new OutputStreamWriter(outputStream, StandardCharsets.UTF_8))
////
////    writer.write("Word,Token,Frequency,Embeddings,Closest Similar Word\n")
////
////    for ((token, (word, frequency)) <- tokenFrequencyMap) {
////      val embedding = embeddingMap.getOrElse(token, "N/A")
////      val closestWords = cosineSimilarityMap.getOrElse(token, "N/A")
////      writer.write(s"$word,$token,$frequency,$embedding,$closestWords\n")
////    }
////
////    writer.close()
////  }
////}
//
//
//
//
////def main(args: Array[String]): Unit = {
////
////  // 1. Data Cleaning and Sharding
////  val inputFilePath = "hdfs:///user/hadoop/input/testing_text.txt" // Update this path to your HDFS input file
////  val numShards = 5 // Number ouser/hadoop/f shards (adjust as needed)
////
////  // Setup Hadoop configuration and FileSystem
////  val conf = new Configuration()
////  val fs = FileSystem.get(conf)
////
////  // Read and clean the input text from HDFS
////  val inputFile = fs.open(new Path(inputFilePath))
////  val testingText = Source.fromInputStream(inputFile).mkString
////  inputFile.close()
////
////  // Perform cleaning and sharding as before
////  val cleanedText = DataCleaner.cleanText(testingText)
////  val shards = Sharder.shardText(cleanedText, numShards)
////
////  // Write the shards to the HDFS output file
////  val shardFilePath = "hdfs:///user/hadoop/output/sharded_text.txt"
////  writeToHDFS(shardFilePath, shards.mkString("\n"), fs)
////  println(s"Sharded text written to: $shardFilePath")
////
////  // 2. Run Token Frequency MapReduce Job
////  val tokenOutputPath = "hdfs:///user/hadoop/output/tokenization_output"
////  val tokenJob = new TokenFrequencyJob()
////  if (tokenJob.runJob(shardFilePath, tokenOutputPath)) {
////    println(s"Token MapReduce job completed successfully. Check the output at: $tokenOutputPath")
////    //      Thread.sleep(5000)
////    // 3. Check Embedding Input in HDFS
////    val embeddingInputPath = "hdfs:///user/hadoop/output/tokens_output_only.txt"
////    try {
////      val input = fs.open(new Path(embeddingInputPath)) // Try to open the file directly
////      input.close() // If opened, close it
////      println(s"Embedding input file exists at: $embeddingInputPath")
////
////      // 4. Run Embedding MapReduce Job
////      val embeddingOutputPath = "hdfs:///user/hadoop/output/embedding_output"
////      val embeddingJob = new EmbeddingJob()
////
////      if (embeddingJob.runJob(embeddingInputPath, embeddingOutputPath)) {
////        println(s"Embedding MapReduce job completed successfully. Check the output at: $embeddingOutputPath")
////
////        // 5. Run Cosine Similarity Job
////        val cosineInputPath = "hdfs:///user/hadoop/output/embedding_output/part-r-00000"
////        val cosineOutputPath = "hdfs:///user/hadoop/output/cosine_similarity_output"
////        val cosineJob = new CosineSimilarityJob()
////
////        if (cosineJob.runJob(cosineInputPath, cosineOutputPath)) {
////          println(s"Cosine Similarity MapReduce job completed successfully. Check the output at: $cosineOutputPath")
////
////          // Generate final CSV output
////          val tokenFilePath = "hdfs:///user/hadoop/output/tokenization_output/part-r-00000"
////          val embeddingFilePath = "hdfs:///user/hadoop/output/embedding_output/part-r-00000"
////          val cosineFilePath = "hdfs:///user/hadoop/output/cosine_similarity_output/part-r-00000"
////          val csvFilePath = "hdfs:///user/hadoop/output/final_output.csv"
////
////          val tokenFrequencyMap = readTokenFile(tokenFilePath, fs)
////          val embeddingMap = readEmbeddingFile(embeddingFilePath, fs)
////          val cosineSimilarityMap = readCosineFile(cosineFilePath, fs)
////
////          writeToCSV(tokenFrequencyMap, embeddingMap, cosineSimilarityMap, csvFilePath, fs)
////          println(s"CSV file created at: $csvFilePath")
////
////        } else {
////          println("Cosine Similarity MapReduce job failed.")
////        }
////      } else {
////        println("Embedding MapReduce job failed.")
////      }
////
////    } catch {
////      case e: IOException =>
////        println(s"Error: Embedding input file not found or could not be opened: $embeddingInputPath. Error: ${e.getMessage}")
////    }
////
////  } else {
////    println("Token MapReduce job failed.")
////  }
////
////  // 6. Create Final CSV Output from HDFS
////
////}
////
////// Helper function to write to HDFS
////def writeToHDFS(filePath: String, content: String, fs: FileSystem): Unit = {
////  val path = new Path(filePath)
////  if (fs.exists(path)) {
////    fs.delete(path, true)
////  }
////  val outputStream = fs.create(path)
////  outputStream.write(content.getBytes(StandardCharsets.UTF_8))
////  outputStream.close()
////}
////
////// Helper function to read from HDFS token file
////def readTokenFile(tokenFilePath: String, fs: FileSystem): Map[Int, (String, Int)] = {
////  val inputFile = fs.open(new Path(tokenFilePath))
////  val lines = Source.fromInputStream(inputFile).getLines().foldLeft(Map[Int, (String, Int)]()) { (map, line) =>
////    val parts = line.split("\\s*,\\s*")
////    val word = parts(0).trim
////    val token = parts(1).split(",")(0).trim.toInt
////    val frequency = parts(2).trim.toInt
////    map.updated(token, (word, frequency))
////  }
////  inputFile.close()
////  lines
////}
////
////def readEmbeddingFile(embeddingFilePath: String, fs: FileSystem): Map[Int, String] = {
////  val inputFile = fs.open(new Path(embeddingFilePath))
////  val lines = Source.fromInputStream(inputFile).getLines().foldLeft(Map[Int, String]()) { (map, line) =>
////    val parts = line.split("\\s+")
////    val token = parts(0).toInt
////    val embedding = parts.drop(1).mkString(",")
////    map.updated(token, embedding)
////  }
////  inputFile.close()
////  lines
////}
////
////def readCosineFile(cosineFilePath: String, fs: FileSystem): Map[Int, String] = {
////  val inputFile = fs.open(new Path(cosineFilePath))
////  val lines = Source.fromInputStream(inputFile).getLines().foldLeft(Map[Int, String]()) { (map, line) =>
////    val parts = line.split("\\s+")
////    val token = parts(0).toInt
////    val closestWords = parts.drop(1).mkString(" ")
////    map.updated(token, closestWords)
////  }
////  inputFile.close()
////  lines
////}
////
////// Helper function to write the CSV to HDFS
////def writeToCSV(
////                tokenFrequencyMap: Map[Int, (String, Int)],
////                embeddingMap: Map[Int, String],
////                cosineSimilarityMap: Map[Int, String],
////                csvFilePath: String,
////                fs: FileSystem
////              ): Unit = {
////  val outputPath = new Path(csvFilePath)
////  val outputStream = fs.create(outputPath, true)
////  val writer = new BufferedWriter(new OutputStreamWriter(outputStream, StandardCharsets.UTF_8))
////
////  writer.write("Word,Token,Frequency,Embeddings,Closest Similar Word\n")
////
////  for ((token, (word, frequency)) <- tokenFrequencyMap) {
////    val embedding = embeddingMap.getOrElse(token, "N/A")
////    val closestWords = cosineSimilarityMap.getOrElse(token, "N/A")
////    writer.write(s"$word,$token,$frequency,$embedding,$closestWords\n")
////  }
////
////  writer.close()
////}
////}
//
//
////
////    // 1. Data Cleaning and Sharding
////    val inputFilePath = "src/main/resources/input/testing_text.txt" // Update this path to your input file
////    val numShards = 5 // Number of shards (adjust as needed)
////    // Read and clean the input text
////    val testingText = new String(Files.readAllBytes(Paths.get(inputFilePath)), StandardCharsets.UTF_8)
////    val cleanedText = DataCleaner.cleanText(testingText)
////    // Shard the text
////    val shards = Sharder.shardText(cleanedText, numShards)
////
////    val tokenFilePath = "resources/output/tokenization_output/part-r-00000"
////    val embeddingFilePath = "resources/output/embedding_output/part-r-00000"
////    val cosineFilePath = "resources/output/cosine_similarity_output/part-r-00000"
////    val csvFilePath = "resources/output/final_output.csv"
////
////    // Write the shards to the output file
////    val outputDir = "resources/output"
////    val shardFilePath = s"$outputDir/sharded_text.txt"
////    writeToFile(shardFilePath, shards.mkString("\n"))
////    println(s"Sharded text written to: $shardFilePath")
////
////    // 2. Run Token Frequency MapReduce Job
////    val tokenOutputPath = "resources/output/tokenization_output"
////    val tokenJob = new TokenFrequencyJob()
////    if (tokenJob.runJob(shardFilePath, tokenOutputPath)) {
////      println(s"Token MapReduce job completed successfully. Check the output at: $tokenOutputPath")
////
////      // 3. Ensure tokens_output_only.txt exists and is valid
////      val embeddingInputPath = s"resources/output/tokens_output_only.txt"
////      if (!Files.exists(Paths.get(embeddingInputPath))) {
////        println(s"Embedding input file not found: $embeddingInputPath. Terminating process.")
////        return
////      }
////
////      // Check if file is empty
////      if (Files.size(Paths.get(embeddingInputPath)) == 0) {
////        println(s"Embedding input file is empty: $embeddingInputPath. Terminating process.")
////        return
////      }
////
////      println(s"Embedding input file found: $embeddingInputPath")
////
////      // 4. Run Embedding MapReduce Job
////      val embeddingOutputPath = "resources/output/embedding_output"
////      val embeddingJob = new EmbeddingJob()
////
////      if (embeddingJob.runJob(embeddingInputPath, embeddingOutputPath)) {
////        println(s"Embedding MapReduce job completed successfully. Check the output at: $embeddingOutputPath")
////        val cosineInputPath = embeddingOutputPath // The embeddings generated from the previous job
////        val cosineOutputPath = "resources/output/cosine_similarity_output"
////        val cosineJob = new CosineSimilarityJob()
////
////        if (cosineJob.runJob(cosineInputPath, cosineOutputPath)) {
////          println(s"Cosine Similarity MapReduce job completed successfully. Check the output at: $cosineOutputPath")
////        } else {
////          println("Cosine Similarity MapReduce job failed.")
////        }
////
////      } else {
////        println("Embedding MapReduce job failed.")
////      }
////
////    } else {
////      println("Token MapReduce job failed.")
////    }
////    val tokenFrequencyMap = readTokenFile(tokenFilePath)
////    val embeddingMap = readEmbeddingFile(embeddingFilePath)
////    val cosineSimilarityMap = readCosineFile(cosineFilePath)
////    writeToCSV(tokenFrequencyMap, embeddingMap, cosineSimilarityMap, csvFilePath)
////    println(s"CSV file created at: $csvFilePath")
////
////  }
//
//
////  // Helper function to write to a file
////  def writeToFile(filePath: String, content: String): Unit = {
////    try {
////      val path = Paths.get(filePath)
////      Files.createDirectories(path.getParent) // Create directories if they don't exist
////      Files.write(path, content.getBytes(StandardCharsets.UTF_8))
////    } catch {
////      case e: IOException =>
////        println(s"Error writing to file: $filePath. Error: ${e.getMessage}")
////    }
////  }
////
////  def readTokenFile(tokenFilePath: String): Map[Int, (String, Int)] = {
////    Source.fromFile(tokenFilePath).getLines().map { line =>
////      val parts = line.split("\\s*,\\s*") // Split by commas with possible whitespace
////      val word = parts(0).trim
////      val token = parts(1).split(",")(0).trim.toInt // Convert the first token part to an integer
////      val frequency = parts(2).trim.toInt
////      token -> (word, frequency)
////    }.toMap
////  }
////
////  def readEmbeddingFile(embeddingFilePath: String): Map[Int, String] = {
////    Source.fromFile(embeddingFilePath).getLines().map { line =>
////      val parts = line.split("\\s+")
////      val token = parts(0).toInt
////      val embedding = parts(1)
////      token -> embedding
////    }.toMap
////  }
////
////  def readCosineFile(cosineFilePath: String): Map[Int, String] = {
////    Source.fromFile(cosineFilePath).getLines().map { line =>
////      val parts = line.split("\\s+")
////      val token = parts(0).toInt
////      val closestWords = parts.drop(1).mkString(" ")
////      token -> closestWords
////    }.toMap
////  }
////
////  def writeToCSV(
////                  tokenFrequencyMap: Map[Int, (String, Int)],
////                  embeddingMap: Map[Int, String],
////                  cosineSimilarityMap: Map[Int, String],
////                  csvFilePath: String
////                ): Unit = {
////    val writer = new BufferedWriter(new FileWriter(csvFilePath))
////    writer.write("Word,Token,Frequency,Embeddings,Closest Similar Word\n")
////
////    for ((token, (word, frequency)) <- tokenFrequencyMap) {
////      val embedding = embeddingMap.getOrElse(token, "N/A")
////      val closestWords = cosineSimilarityMap.getOrElse(token, "N/A")
////      writer.write(s"$word,$token,$frequency,$embedding,$closestWords\n")
////    }
////
////    writer.close()
////  }
////
////
////}
//
//
//
//
