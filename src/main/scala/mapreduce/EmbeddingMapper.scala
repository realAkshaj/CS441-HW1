package mapreduce

import org.apache.hadoop.io.{LongWritable, Text}
import org.apache.hadoop.mapreduce.Mapper
import embedding.EmbeddingPreprocessor
import org.nd4j.linalg.api.ndarray.INDArray
import org.slf4j.LoggerFactory

import scala.collection.mutable.ListBuffer
import scala.util.Try

class EmbeddingMapper extends Mapper[LongWritable, Text, Text, Text] {

  private val logger = LoggerFactory.getLogger(classOf[EmbeddingMapper])
  private val collectedTokens = ListBuffer[Int]() // Token buffer
  private val batchSize = 1000 // Process tokens in batches (adjust size as needed)

  // The map method collects tokens and processes them in batches
  override def map(key: LongWritable, value: Text, context: Mapper[LongWritable, Text, Text, Text]#Context): Unit = {
    val line = value.toString.trim
    // Split by comma first to separate tokens, then handle any trailing tabs/spaces
    val parts = line.split(",").map(_.trim)

    // Iterate over parts to extract tokens (assuming tokens are numeric)
    parts.foreach { part =>
      // Remove any trailing tabs or non-numeric characters
      val cleanedPart = part.replaceAll("\\t", "")
      if (isValidNumber(cleanedPart)) {
        val token = cleanedPart.toInt
        collectedTokens += token

        // Process tokens in batches if batch size is reached
        if (collectedTokens.size >= batchSize) {
          processBatch(context)
        }
      } else {
        logger.warn(s"Invalid token: '$cleanedPart' in line: '$line'")
      }
    }
  }

  // The cleanup method is called once at the end of processing the shard
  override def cleanup(context: Mapper[LongWritable, Text, Text, Text]#Context): Unit = {
    logger.info("Mapper task finished. Cleanup called.")
    // Process any remaining tokens that didn't form a full batch
    if (collectedTokens.nonEmpty) {
      processBatch(context)
    }
  }

  // Helper function to process batch of tokens
  private def processBatch(context: Mapper[LongWritable, Text, Text, Text]#Context): Unit = {
    // Generate embeddings for collected tokens
    val embeddings: Map[Int, INDArray] = EmbeddingPreprocessor.generateEmbeddingsForTokens(collectedTokens.toSeq, windowSize = 3, stride = 1)

    embeddings.foreach { case (token, embeddingVector) =>
      val embeddingStr = embeddingVector.toDoubleVector.mkString(",")
      context.write(new Text(token.toString), new Text(embeddingStr))
    }

    collectedTokens.clear() // Clear the token buffer after processing the batch
  }

  // Helper function to check if a string is a valid number
  def isValidNumber(str: String): Boolean = {
    Try(str.toInt).isSuccess
  }
}

