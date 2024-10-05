package tokenizer

import com.knuddels.jtokkit.Encodings
import com.knuddels.jtokkit.api.{Encoding, EncodingRegistry, IntArrayList, ModelType}
import org.slf4j.LoggerFactory
import org.nd4j.linalg.api.ndarray.INDArray
import org.nd4j.linalg.factory.Nd4j

object Tokenizer {


  private val logger = LoggerFactory.getLogger(getClass)

  // Initialize JTokkit's encoding registry
  private val encodingRegistry: EncodingRegistry = Encodings.newDefaultEncodingRegistry()
  private val encoding: Encoding = encodingRegistry.getEncodingForModel(ModelType.GPT_4O)

  // Method to generate an embedding for a given token
  def tokenize(word: String): Array[Int] = {
    val tokens: IntArrayList = encoding.encode(word) // Tokenize the word using JTokkit
    // Use map to create the array from IntArrayList
    (0 until tokens.size()).map(tokens.get).toArray // Convert to Array[Int]
  }
}
//  def tokenizeDataset(dataset: Seq[String]): Map[String, Array[Int]] = {
//    dataset.map(word => word -> tokenize(word)).toMap // Map each word to its tokens
//  }
//
//  def getEmbeddings(token: Int): INDArray = {
//    // Create a dummy embedding (replace this with actual embedding logic)
//    val embeddingArray = Array(0.0f, 1.0f, 2.0f) // Replace with actual logic to fetch embeddings for the token
//    Nd4j.create(embeddingArray)
//  }


