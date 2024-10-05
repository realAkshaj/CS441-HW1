package mapreduce

import org.nd4j.linalg.api.ndarray.INDArray
import org.apache.hadoop.io.{LongWritable, Text}
import org.apache.hadoop.mapreduce.{Mapper, Reducer}
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat
import org.apache.hadoop.mapreduce.Job
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.nd4j.linalg.factory.Nd4j
import org.slf4j.LoggerFactory

import scala.collection.mutable.ListBuffer

class CosineSimilarityMapper extends Mapper[LongWritable, Text, Text, Text] {

  private val logger = LoggerFactory.getLogger(classOf[CosineSimilarityMapper])

  val embeddingsBuffer: ListBuffer[(String, Array[Double])] = ListBuffer()

  override def map(key: LongWritable, value: Text, context: Mapper[LongWritable, Text, Text, Text]#Context): Unit = {
    val line = value.toString.trim
    val tokens = line.split("\t")

    if (tokens.length == 2) {
      val word = tokens(0)
      val embeddingStr = tokens(1)

      try {
        // Parse the embedding into an array of doubles
        val embedding = embeddingStr.split(",").map(_.toDouble)
        embeddingsBuffer += ((word, embedding))

      } catch {
        case e: NumberFormatException =>
          logger.error(s"Failed to parse embedding for word '$word': ${e.getMessage}")
      }
    } else {
      logger.error(s"Invalid input format: $line")
    }
  }

  override def cleanup(context: Mapper[LongWritable, Text, Text, Text]#Context): Unit = {
    // Calculate cosine similarity between each embedding and every other embedding
    for (i <- embeddingsBuffer.indices) {
      val (word1, vec1) = embeddingsBuffer(i)
      val similarities = new StringBuilder

      for (j <- embeddingsBuffer.indices if i != j) {
        val (word2, vec2) = embeddingsBuffer(j)
        val similarity = cosineSimilarity(vec1, vec2)
        similarities.append(s"$word2:$similarity; ")
      }

      // Write word and its similarities to context
      context.write(new Text(word1), new Text(similarities.toString()))
    }
  }

  // Helper function to calculate cosine similarity
  def cosineSimilarity(vec1: Array[Double], vec2: Array[Double]): Double = {
    val dotProduct = vec1.zip(vec2).map { case (a, b) => a * b }.sum
    val magnitude1 = math.sqrt(vec1.map(x => x * x).sum)
    val magnitude2 = math.sqrt(vec2.map(x => x * x).sum)
    if (magnitude1 == 0.0 || magnitude2 == 0.0) 0.0
    else dotProduct / (magnitude1 * magnitude2)
  }
}






